import asyncio
import schedule
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import logging
from typing import Callable, List, Dict, Optional, Tuple, Any
import sys
import os
import ssl
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException
from functools import wraps

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from data_collection.historical_data import HistoricalData
from data_collection.new_announcement import NewAnnouncement
from data_collection.stock_details import StockDetails
from data_collection.ipo_data import IpoData
from data_collection.price_shockers import PriceShockers
from utils.database.retrival_utils import get_all_securities_ids

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_collection.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def with_retry(max_retries: int = 3, delay: float = 1.0):
    """Decorator to implement retry logic with exponential backoff"""
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    if attempt > 0:
                        await asyncio.sleep(delay * (2 ** (attempt - 1)))
                    
                    if not self.client or not self.is_connected():
                        await self.connect()
                    
                    result = await func(self, *args, **kwargs)
                    self.update_activity()
                    return result
                
                except (ConnectionClosed, WebSocketException, ssl.SSLError, AttributeError) as e:
                    last_exception = e
                    logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {self.name}: {str(e)}")
                    await self.close()
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error in {self.name}: {str(e)}")
                    raise
            
            raise last_exception or Exception(f"All {max_retries} attempts failed")
        return wrapper
    return decorator

class WebSocketClient:
    def __init__(self, name: str, create_func: Callable, uri: str):
        self.name = name
        self.create_func = create_func
        self.uri = uri
        self.client = None
        self.last_activity = None
        self.ping_interval = 30  # 30 seconds
        self.ping_timeout = 10   # 10 seconds
        self.reconnect_delay = 5 # 5 seconds
        self.ssl_context = self._create_ssl_context()
        self._lock = asyncio.Lock()
        self._ping_task = None

    def _create_ssl_context(self) -> ssl.SSLContext:
        """Create a custom SSL context with appropriate settings"""
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context

    def is_connected(self) -> bool:
        """Check if the WebSocket connection is active"""
        return (
            self.client is not None and 
            hasattr(self.client, 'transport') and 
            self.client.transport is not None and 
            not getattr(self.client, 'closed', True) and
            self.last_activity is not None and
            time.time() - self.last_activity < self.ping_interval
        )

    async def start_ping(self):
        """Start the ping task"""
        if self._ping_task is not None:
            self._ping_task.cancel()
        self._ping_task = asyncio.create_task(self._ping_loop())

    async def _ping_loop(self):
        """Keep the connection alive with periodic pings"""
        try:
            while self.is_connected():
                await asyncio.sleep(self.ping_interval)
                if self.is_connected():
                    try:
                        pong_waiter = await self.client.ping()
                        await asyncio.wait_for(pong_waiter, timeout=self.ping_timeout)
                        self.update_activity()
                    except (asyncio.TimeoutError, ConnectionClosed):
                        logger.warning(f"{self.name} ping failed, reconnecting...")
                        await self.close()
                        break
        except Exception as e:
            logger.error(f"Error in ping loop for {self.name}: {str(e)}")
        finally:
            await self.close()

    async def connect(self) -> bool:
        """Establish WebSocket connection with retry logic"""
        async with self._lock:
            if self.is_connected():
                return True

            try:
                self.client = await self.create_func()
                self.last_activity = time.time()
                await self.start_ping()
                logger.info(f"{self.name} connected successfully")
                return True
            except Exception as e:
                logger.error(f"Error connecting {self.name}: {str(e)}")
                await self.close()
                return False

    @with_retry(max_retries=3, delay=1.0)
    async def send_message(self, message: str) -> Tuple[bool, Any]:
        """Send a message with automatic reconnection"""
        try:
            await self.client.send(message)
            response = await self.client.recv()
            return True, response
        except Exception as e:
            logger.error(f"Error sending message in {self.name}: {str(e)}")
            raise

    async def close(self):
        """Close the WebSocket connection and cleanup resources"""
        async with self._lock:
            if self._ping_task:
                self._ping_task.cancel()
                self._ping_task = None

            if self.client:
                try:
                    await self.client.close()
                except Exception as e:
                    logger.error(f"Error closing {self.name}: {str(e)}")
                finally:
                    self.client = None
                    self.last_activity = None

    def update_activity(self):
        """Update last activity timestamp"""
        self.last_activity = time.time()

    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()

class DataCollectionScheduler:
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=3)
        self.running = True
        
        # Initialize WebSocket clients with their respective URIs
        self.ws_clients = {
            'historical': WebSocketClient(
                name='Historical Data',
                create_func=HistoricalData.create,
                uri="wss://analyst-ws.indianapi.in/ws/historical_data"
            ),
            'announcement': WebSocketClient(
                name='Announcements',
                create_func=NewAnnouncement.create,
                uri="wss://analyst-ws.indianapi.in/ws/recent_announcements"
            ),
            'stock_details': WebSocketClient(
                name='Stock Details',
                create_func=StockDetails.create,
                uri="wss://analyst-ws.indianapi.in/ws/stock"
            )
        }

    async def process_historical_data(self, security_code: str) -> None:
        """Process historical data with improved error handling"""
        client = self.ws_clients['historical']
        
        try:
            periods = ["1m", "6m", "1yr", "3yr", "5yr", "10yr", "max"]
            filters = ["price", "pe", "sm", "evebitda", "ptb", "mcs"]
            
            async with client as ws:
                for period in periods:
                    for filter_type in filters:
                        try:
                            success, response = await ws.send_message(
                                f'{{"stock_name": "{security_code}", "period": "{period}", "filter": "{filter_type}"}}'
                            )
                            if success:
                                logger.info(f"Historical Data {security_code} - {period} - {filter_type}: Success")
                            await asyncio.sleep(0.5)
                        except Exception as e:
                            logger.error(f"Error processing historical data for {security_code} - {period} - {filter_type}: {str(e)}")
                            await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Failed to process historical data for {security_code}: {str(e)}")

    async def process_announcements(self, security_code: str) -> None:
        """Process announcements with improved error handling"""
        client = self.ws_clients['announcement']
        
        try:
            async with client as ws:
                success, response = await ws.send_message(
                    f'{{"stock_name": "{security_code}"}}'
                )
                if success:
                    logger.info(f"Announcement {security_code}: Success")
                await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(f"Failed to process announcements for {security_code}: {str(e)}")

    async def process_stock_details(self, security_code: str) -> None:
        """Process stock details with improved error handling"""
        client = self.ws_clients['stock_details']
        
        try:
            async with client as ws:
                success, response = await ws.send_message(
                    f'{{"name": "{security_code}"}}'
                )
                if success:
                    logger.info(f"Stock Details {security_code}: Success")
                await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(f"Failed to process stock details for {security_code}: {str(e)}")

    async def process_security(self, security_code: str) -> None:
        """Process all data for a single security"""
        tasks = [
            self.process_historical_data(security_code),
            self.process_announcements(security_code),
            self.process_stock_details(security_code)
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(1)

    async def run_continuous_collection(self, security_codes: List[str]) -> None:
        """Run continuous data collection with improved connection handling"""
        batch_size = 5
        
        while self.running:
            try:
                for i in range(0, len(security_codes), batch_size):
                    batch = security_codes[i:i + batch_size]
                    
                    for security_code in batch:
                        await self.process_security(security_code)
                    
                    await asyncio.sleep(2)
                
                logger.info("Completed processing all securities. Waiting before next iteration...")
                await asyncio.sleep(300)
                
            except Exception as e:
                logger.error(f"Error in continuous collection: {str(e)}")
                await self.close_all_connections()
                await asyncio.sleep(60)

    def run_daily_collection(self) -> None:
        """Run daily data collection tasks"""
        try:
            # IPO Data
            ipo = IpoData()
            ipo_data = ipo._fetch_data()
            if ipo_data:
                ipo._upload_data(ipo_data)
                logger.info("IPO data collection completed successfully")
        except Exception as e:
            logger.error(f"Error in IPO data collection: {str(e)}")

        try:
            # Price Shockers
            shockers = PriceShockers()
            shockers_data = shockers._fetch_data()
            if shockers_data:
                shockers._upload_data(shockers_data)
                logger.info("Price shockers collection completed successfully")
        except Exception as e:
            logger.error(f"Error in price shockers collection: {str(e)}")

    def schedule_daily_tasks(self) -> None:
        """Schedule daily tasks"""
        schedule.every().day.at("00:00").do(self.run_daily_collection)

    async def close_all_connections(self) -> None:
        """Close all WebSocket connections"""
        for client in self.ws_clients.values():
            await client.close()

    async def start(self) -> None:
        """Start the scheduler with improved error handling"""
        try:
            security_ids = ["AIML"]
            # security_ids = get_all_securities_ids()
            if not security_ids:
                raise ValueError("No security codes found")

            self.schedule_daily_tasks()

            continuous_task = asyncio.create_task(
                self.run_continuous_collection(security_ids)
            )

            async def check_schedule():
                while self.running:
                    schedule.run_pending()
                    await asyncio.sleep(60)

            schedule_task = asyncio.create_task(check_schedule())

            await asyncio.gather(continuous_task, schedule_task)

        except Exception as e:
            logger.error(f"Error in scheduler: {str(e)}")
            await self.stop()

    async def stop(self) -> None:
        """Stop the scheduler gracefully"""
        self.running = False
        await self.close_all_connections()
        self.executor.shutdown(wait=True)

async def main():
    scheduler = DataCollectionScheduler()
    try:
        await scheduler.start()
    except KeyboardInterrupt:
        logger.info("Shutting down scheduler...")
        await scheduler.stop()

if __name__ == "__main__":
    asyncio.run(main())