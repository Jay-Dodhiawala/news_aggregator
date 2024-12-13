import websockets
import json
import os
import sys
import asyncio
import hashlib
from typing import Tuple
import backoff

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.database.retrival_utils import list_all_storage_files, get_bse_ids
from utils.supabase_client import supabase_client
from bse_data_collection import get_all_data_from_name
from dotenv import load_dotenv

load_dotenv()

# class HistoricalData:
#     BASE_URL = "wss://analyst-ws.indianapi.in"

#     def __init__(self):
#         self.header = {
#             "X-API-Key": os.getenv("INDIAN_STOCK_MARKET_API_KEY")
#         }
#         self.full_url = f"{self.BASE_URL}/ws/historical_data"
#         self.websocket = None
#         self.supabase = supabase_client

#         self.storage_name = "pdf_chunks"
#         self.folder_name = "test_historical_data"
#         self.existing_files = list_all_storage_files(self.storage_name, self.folder_name)

#     @classmethod
#     async def create(cls):
#         self = cls()
#         self.websocket = await websockets.connect(self.full_url, extra_headers=self.header)
#         return self

#     async def close_connection(self):
#         if self.websocket:
#             await self.websocket.close(code=1000, reason="Closing connection")

#     def _generate_hash(self, content: str) -> str:
#         """Generate SHA-256 hash of the content"""
#         return hashlib.sha256(content.encode('utf-8')).hexdigest()

#     def _check_existing_hash(self, content_hash: str) -> bool:
#         """Check if the hash already exists in the database"""
#         try:
#             response = self.supabase.table('historical_data').select('hash').eq('hash', content_hash).execute()
#             return len(response.data) > 0
#         except Exception as e:
#             print(f"Error checking hash existence: {e}")
#             return False

#     async def _insert_historical_data(self, stock_name: str, period:str, filter:str, content: str, content_hash: str) -> bool:
#         """Insert new stock details into the database and storage"""
#         try:
#             filename = f"{stock_name}_{period}_{filter}.txt"
#             # Define the file path in storage
#             blob_file_path = f"{self.folder_name}/{filename}.txt"
            
#             # Check if file exists in storage
#             try:
#                 # existing_files = self.supabase.storage.from_("pdf_chunks").list("test_historical_data")
#                 file_exists = any(file['name'] == f"{filename}.txt" for file in self.existing_files)
                
#                 if file_exists:
#                     # Update existing file
#                     result = self.supabase.storage.from_("pdf_chunks").update(
#                         blob_file_path,
#                         content.encode('utf-8'),
#                         {
#                             'contentType': 'text/plain'
#                         }
#                     )
#                     print(f"File updated successfully for {filename}")
#                 else:
#                     # Upload new file
#                     result = self.supabase.storage.from_("pdf_chunks").upload(
#                         blob_file_path,
#                         content.encode('utf-8'),
#                         {
#                             'contentType': 'text/plain'
#                         }
#                     )
#                     print(f"File uploaded successfully for {filename}")

#             except Exception as storage_error:
#                 print(f"Storage operation failed: {storage_error}")
#                 return False

#             # Update database record
#             data = {
#                 "stock_name": stock_name.upper(),
#                 "period": period,
#                 "filter": filter,
#                 "hash": content_hash,
#                 "content": blob_file_path
#             }

#             response = self.supabase.table('historical_data').upsert(data).execute()
#             return len(response.data) > 0
            
#         except Exception as e:
#             print(f"Error in _insert_historical_data: {e}")
#             return False

#     async def check_and_update_historical_data(self, stock_name: str, period:str, filter:str) -> Tuple[bool, str]:
#         """
#         Check for new announcements and update if new hash is found

#         period: str - 1m, 6m, 1yr, 3yr, 5yr, 10yr, max
#         filter: str - price, pe, sm, evebitda, ptb, mcs
        
#         Returns:
#         Tuple[bool, str]: (success, message)
#         """
#         try:
#             # Send request for announcements
#             message = f'{{"stock_name": "{stock_name}", "period": "{period}", "filter": "{filter}"}}'
#             await self.websocket.send(message)
            
#             # Get response
#             response = await self.websocket.recv()
            
#             # Generate hash of the raw response
#             content_hash = self._generate_hash(response)
            
#             # Check if this hash already exists
#             if self._check_existing_hash(content_hash):
#                 return False, "Details already exist"
            

#             # If hash doesn't exist, insert the new announcement
#             success = await self._insert_historical_data(stock_name, period, filter, response, content_hash)
            
#             if success:
#                 return True, "New Details added successfully"
#             else:
#                 return False, "Failed to add details"
                
#         except Exception as e:
#             return False, f"Error processing stock details: {str(e)}"


# periods = ['1m', '6m', '1yr', '3yr', '5yr', '10yr', 'max']
# filters = ['price', 'pe', 'sm', 'evebitda', 'ptb', 'mcs']
# ids = get_bse_ids()
# # Example usage:
# async def main():
#     checker = await HistoricalData.create()
#     try:
#         for id in ids:
#             for period in periods:
#                 for filter in filters:
#                     success, message = await checker.check_and_update_historical_data(id, period, filter)
#                     print(f"Status: {success}, Message: {message}")
#             # success, message = await checker.check_and_update_historical_data("TCS", "6m", "price")
#             # print(f"Status: {success}, Message: {message}")
#     finally:
#         await checker.close_connection()

# if __name__ == "__main__":
#     asyncio.run(main())











import websockets
import json
import os
import sys
import asyncio
import hashlib
from typing import Tuple, Optional
import backoff  # Add to requirements.txt

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.database.retrival_utils import get_document_ids_from_name
from utils.supabase_client import supabase_client
from bse_data_collection import get_all_data_from_name
from dotenv import load_dotenv

load_dotenv()

class HistoricalData:
    BASE_URL = "wss://analyst-ws.indianapi.in"
    MAX_RETRIES = 3
    PING_INTERVAL = 30  # seconds
    PING_TIMEOUT = 10   # seconds

    def __init__(self):
        self.header = {
            "X-API-Key": os.getenv("INDIAN_STOCK_MARKET_API_KEY")
        }
        self.full_url = f"{self.BASE_URL}/ws/historical_data"
        self.websocket = None
        self.supabase = supabase_client
        self._ping_task = None
        self._closed = False

    @classmethod
    async def create(cls):
        self = cls()
        await self._connect()
        return self

    async def _connect(self):
        """Establish websocket connection with ping/pong handling"""
        try:
            self.websocket = await websockets.connect(
                self.full_url,
                extra_headers=self.header,
                ping_interval=self.PING_INTERVAL,
                ping_timeout=self.PING_TIMEOUT
            )
            self._closed = False
            self._ping_task = asyncio.create_task(self._keepalive())
        except Exception as e:
            print(f"Connection error: {e}")
            raise

    async def _keepalive(self):
        """Keep the connection alive with periodic pings"""
        try:
            while not self._closed:
                await asyncio.sleep(self.PING_INTERVAL)
                try:
                    pong_waiter = await self.websocket.ping()
                    await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
                except Exception as e:
                    print(f"Ping failed: {e}")
                    await self._reconnect()
        except asyncio.CancelledError:
            pass

    async def _reconnect(self):
        """Reconnect to the websocket"""
        print("Attempting to reconnect...")
        try:
            await self.close_connection()
            await self._connect()
        except Exception as e:
            print(f"Reconnection failed: {e}")
            raise

    async def close_connection(self):
        """Properly close the websocket connection"""
        self._closed = True
        if self._ping_task:
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass
        if self.websocket:
            await self.websocket.close()
            self.websocket = None

    def _generate_hash(self, content: str) -> str:
        """Generate SHA-256 hash of the content"""
        return hashlib.sha256(content.encode('utf-8')).hexdigest()

    def _check_existing_hash(self, content_hash: str) -> bool:
        """Check if the hash already exists in the database"""
        try:
            response = self.supabase.table('historical_data').select('hash').eq('hash', content_hash).execute()
            return len(response.data) > 0
        except Exception as e:
            print(f"Error checking hash existence: {e}")
            return False

    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    def _insert_historical_data(self, stock_name: str, period: str, filter: str, content: str, content_hash: str) -> bool:
        """Insert new stock details into the database and storage with retry logic"""
        try:
            filename = f"{stock_name}_{period}_{filter}"
            blob_file_path = f"test_historical_data/{filename}.txt"
            
            try:
                # Check if file exists
                existing_files = self.supabase.storage.from_("pdf_chunks").list("test_historical_data")
                file_exists = any(file['name'] == f"{filename}.txt" for file in existing_files)
                
                if file_exists:
                    # Update existing file
                    self.supabase.storage.from_("pdf_chunks").update(
                        blob_file_path,
                        content.encode('utf-8'),
                        {
                            'contentType': 'text/plain'
                        }
                    )
                    print(f"File updated successfully for {filename}")
                else:
                    # Upload new file
                    self.supabase.storage.from_("pdf_chunks").upload(
                        blob_file_path,
                        content.encode('utf-8'),
                        {
                            'contentType': 'text/plain'
                        }
                    )
                    print(f"File uploaded successfully for {filename}")

            except Exception as storage_error:
                print(f"Storage operation failed: {storage_error}")
                return False

            # Update database record
            data = {
                "stock_name": stock_name.upper(),
                "period": period,
                "filter": filter,
                "hash": content_hash,
                "content": blob_file_path
            }

            response = self.supabase.table('historical_data').upsert(data).execute()
            return len(response.data) > 0
            
        except Exception as e:
            print(f"Error in _insert_historical_data: {e}")
            return False

    async def check_and_update_historical_data(self, stock_name: str, period: str, filter: str, retry_count: int = 0) -> Tuple[bool, str]:
        """
        Check for new data and update if new hash is found with retry logic
        
        Parameters:
        - stock_name: Stock symbol
        - period: 1m, 6m, 1yr, 3yr, 5yr, 10yr, max
        - filter: price, pe, sm, evebitda, ptb, mcs
        - retry_count: Current retry attempt
        
        Returns:
        - Tuple[bool, str]: (success, message)
        """
        try:
            if retry_count >= self.MAX_RETRIES:
                return False, "Max retries exceeded"

            if not self.websocket or self.websocket.closed:
                await self._reconnect()

            message = json.dumps({
                "stock_name": stock_name,
                "period": period,
                "filter": filter
            })
            
            await self.websocket.send(message)
            response = await asyncio.wait_for(self.websocket.recv(), timeout=30)
            
            content_hash = self._generate_hash(response)
            
            if self._check_existing_hash(content_hash):
                return False, "Details already exist"

            success = self._insert_historical_data(stock_name, period, filter, response, content_hash)
            
            if success:
                return True, "New Details added successfully"
            return False, "Failed to add details"
                
        except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError) as e:
            print(f"Connection error (attempt {retry_count + 1}): {e}")
            await self._reconnect()
            return await self.check_and_update_historical_data(stock_name, period, filter, retry_count + 1)
        except Exception as e:
            print(f"Error processing stock details: {e}")
            return False, f"Error processing stock details: {str(e)}"

async def process_stock_data(stock_names: list, periods: list, filters: list):
    """Process multiple stocks with different periods and filters"""
    checker = await HistoricalData.create()
    try:
        for stock in stock_names:
            for period in periods:
                for filter_type in filters:
                    try:
                        success, message = await checker.check_and_update_historical_data(stock, period, filter_type)
                        print(f"Stock: {stock}, Period: {period}, Filter: {filter_type}")
                        print(f"Status: {success}, Message: {message}")
                        await asyncio.sleep(1)  # Add small delay between requests
                    except Exception as e:
                        print(f"Error processing {stock} {period} {filter_type}: {e}")
    finally:
        await checker.close_connection()

# Example usage:
async def main():
    stock_names = get_bse_ids()
    # stock_names = ["TCS", "INFY", "WIPRO"]
    periods = ['1m', '6m', '1yr', '3yr', '5yr', '10yr', 'max']
    filters = ['price', 'pe', 'sm', 'evebitda', 'ptb', 'mcs']
    
    await process_stock_data(stock_names, periods, filters)

if __name__ == "__main__":
    asyncio.run(main())