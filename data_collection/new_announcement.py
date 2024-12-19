import websockets
import json
import os
import sys
import asyncio
import hashlib
from typing import Tuple, Optional
import backoff
import requests
import io
import time
import logging
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.supabase_client import supabase_client
from utils.queue_processor import announcement_queue, queue_processor
from data_collection.bse_data_collection import get_all_data_from_name, download_pdf_from_link, generate_summary

import requests
from requests.adapters import HTTPAdapter, Retry
import io
from pdfminer.high_level import extract_text
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.database.retrival_utils import get_document_ids_from_name, get_bse_ids
from utils.supabase_client import supabase_client
from data_collection.bse_data_collection import get_all_data_from_name, download_pdf_from_link, generate_summary
from dotenv import load_dotenv

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NewAnnouncement:
    BASE_URL = "wss://analyst-ws.indianapi.in"
    MAX_RETRIES = 3
    PING_INTERVAL = 30  # seconds
    PING_TIMEOUT = 10   # seconds

    def __init__(self):
        self.header = {
            "X-API-Key": os.getenv("INDIAN_STOCK_MARKET_API_KEY")
        }
        self.full_url = f"{self.BASE_URL}/ws/recent_announcements"
        self.websocket = None
        self.supabase = supabase_client
        self._ping_task = None
        self._closed = False

    @classmethod
    async def create(cls):
        logger.info("Creating new NewAnnouncement instance")
        self = cls()
        await self._connect()
        return self

    async def _connect(self):
        """Establish websocket connection"""
        logger.info("Attempting to connect to websocket")
        try:
            self.websocket = await websockets.connect(
                self.full_url,
                extra_headers=self.header,
                ping_interval=None,
                ping_timeout=None
            )
            logger.info("Successfully connected to websocket")
            self._closed = False
        except Exception as e:
            logger.error(f"Connection error: {e}")
            raise

    async def check_and_update_announcements(self, stock_name: str, retry_count: int = 0) -> Tuple[bool, str]:
        """Check for new announcements and update if new hash is found"""
        logger.info(f"Processing announcements for {stock_name}")
        try:
            if retry_count >= self.MAX_RETRIES:
                logger.warning(f"Max retries exceeded for {stock_name}")
                return False, "Max retries exceeded"

            if not self.websocket or self.websocket.closed:
                logger.info(f"Reconnecting websocket for {stock_name}")
                await self._connect()

            message = json.dumps({"stock_name": stock_name})
            logger.info(f"Sending request for {stock_name}")
            
            # Send with timeout
            try:
                await asyncio.wait_for(self.websocket.send(message), timeout=10)
                logger.info(f"Request sent for {stock_name}, waiting for response")
                response = await asyncio.wait_for(self.websocket.recv(), timeout=30)
                logger.info(f"Received response for {stock_name}")
            except asyncio.TimeoutError:
                logger.error(f"Timeout while processing {stock_name}")
                if retry_count < self.MAX_RETRIES:
                    await asyncio.sleep(2)
                    return await self.check_and_update_announcements(stock_name, retry_count + 1)
                return False, "Timeout waiting for response"
            
            # Generate hash and check existence
            content_hash = self._generate_hash(response)
            if self._check_existing_hash(content_hash):
                logger.info(f"No new announcements for {stock_name}")
                return False, "Announcement already exists"
            
            # Process response
            try:
                response_data = json.loads(response)
                logger.info(f"Processing {len(response_data)} announcements for {stock_name}")
                processed_announcements = response_data
                # processed_announcements = self.process_announcement(response_data)
                
                # Insert announcement
                success = await self._insert_announcement(stock_name, response, content_hash, processed_announcements)
                
                if success:
                    logger.info(f"Successfully added new announcement for {stock_name}")
                    return True, "New announcement added successfully"
                else:
                    logger.warning(f"Failed to add announcement for {stock_name}")
                    return False, "Failed to add announcement"
                
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON response for {stock_name}: {e}")
                return False, "Invalid response format"
                
        except Exception as e:
            logger.error(f"Error processing {stock_name}: {e}")
            if retry_count < self.MAX_RETRIES:
                await asyncio.sleep(2)
                return await self.check_and_update_announcements(stock_name, retry_count + 1)
            return False, f"Error: {str(e)}"

    def _generate_hash(self, content: str) -> str:
        return hashlib.sha256(content.encode('utf-8')).hexdigest()

    def _check_existing_hash(self, content_hash: str) -> bool:
        try:
            response = self.supabase.table('recent_announcements').select('hash').eq('hash', content_hash).execute()
            return len(response.data) > 0
        except Exception as e:
            logger.error(f"Error checking hash: {e}")
            return False

    def process_announcement(self, response: dict):
        """Modified process_announcement method to handle PDF download issues"""
        processed_announcements = []
        
        for announcement in response:
            try:
                pdf_link = announcement['link']
                logger.info(f"Processing announcement: {announcement.get('title', 'No Title')}")
                logger.info(f"PDF Link: {pdf_link}")
                
                # Download PDF with timeout handling
                pdf_text = download_pdf_from_link(pdf_link)
                
                if not pdf_text:
                    logger.warning(f"No text extracted from PDF: {pdf_link}")
                    # Skip generating summary if PDF download/extraction failed
                    processed_announcement = {
                        "title": announcement['title'],
                        "link": announcement['link'],
                        # "summary": "PDF processing failed"
                    }
                else:
                    # Generate summary only if we have PDF text
                    # summary = generate_summary(pdf_text)
                    processed_announcement = {
                        "title": announcement['title'],
                        "link": announcement['link'],
                        # "summary": summary
                    }
                
                processed_announcements.append(processed_announcement)
                
            except Exception as e:
                logger.error(f"Error processing announcement: {e}")
                # Add failed announcement with error message
                processed_announcements.append({
                    "title": announcement.get('title', 'Unknown Title'),
                    "link": announcement.get('link', 'Unknown Link'),
                    # "summary": f"Processing failed: {str(e)}"
                })
                
        return processed_announcements

    async def _insert_announcement(self, stock_name: str, content: str, content_hash: str, summary: list[dict]) -> bool:
        try:
            logger.info(f"Updating chunks table for {stock_name}")
            get_all_data_from_name(stock_name)

            logger.info(f"Inserting announcement for {stock_name}")
            data = {
                "stock_name": stock_name.upper(),
                "hash": content_hash,
                "content": content,
                # "summary": summary
            }
            response = self.supabase.table('recent_announcements').upsert(data).execute()
            return len(response.data) > 0
        except Exception as e:
            logger.error(f"Error inserting announcement: {e}")
            return False

    async def close_connection(self):
        """Close the websocket connection"""
        logger.info("Closing connection")
        if self.websocket:
            await self.websocket.close()
        self._closed = True

async def process_stock_announcements(stock_names: list):
    """Process announcements for multiple stocks"""
    start_time = datetime.now()
    logger.info(f"Starting processing at {start_time}")
    
    checker = await NewAnnouncement.create()
    total_stocks = len(stock_names)
    
    try:
        for idx, stock in enumerate(stock_names, 1): #1328, 1364
        # for idx, stock in enumerate(stock_names[1923:], 1924): #1328, 1500, 1668
            try:
                logger.info(f"Processing stock {idx}/{total_stocks}: {stock}")
                success, message = await checker.check_and_update_announcements(stock)
                logger.info(f"Stock: {stock} - Status: {success}, Message: {message}")
                
                # Add progress information
                elapsed_time = datetime.now() - start_time
                avg_time_per_stock = elapsed_time / (idx - 809)
                remaining_stocks = total_stocks - idx
                estimated_time_remaining = avg_time_per_stock * remaining_stocks
                
                logger.info(f"Progress: {idx}/{total_stocks} stocks processed")
                logger.info(f"Estimated time remaining: {estimated_time_remaining}")
                
                await asyncio.sleep(1)  # Prevent rate limiting
            
            except Exception as e:
                logger.error(f"Error processing {stock}: {e}")
    finally:
        await checker.close_connection()

    async def _insert_announcement(self, stock_name: str, content: str, content_hash: str, summary: list[dict]) -> bool:
        try:
            logger.info(f"Adding {stock_name} to processing queue")
            await announcement_queue.add_item(stock_name)

            logger.info(f"Inserting announcement for {stock_name}")
            data = {
                "stock_name": stock_name.upper(),
                "hash": content_hash,
                "content": content,
            }
            response = self.supabase.table('recent_announcements').upsert(data).execute()
            return len(response.data) > 0
        except Exception as e:
            logger.error(f"Error inserting announcement: {e}")
            return False


async def main():
    start = time.time()
    stock_names = get_bse_ids()
    logger.info(f"Starting processing for {len(stock_names)} stocks")
    
    try:
        # Start queue processor in the background
        processor_task = asyncio.create_task(queue_processor())
        
        # Start announcement monitoring
        await process_stock_announcements(stock_names)
        
        # Wait for queue to be empty and stop processor
        await announcement_queue.queue.join()
        announcement_queue.stop()
        await processor_task
        
    except Exception as e:
        logger.error(f"Main error: {e}")
    finally:
        announcement_queue.stop()

    print(f"Time taken: {time.time()-start}")
    logger.info("Processing completed")

if __name__ == "__main__":
    asyncio.run(main())

# async def main():
#     start = time.time()
#     # Get list of stock IDs
#     stock_names = get_bse_ids()  # Use your actual stock list
#     # stock_names = ["PURPLEFIN", "ARIES", "NATIONSTD", "BATLIBOI", "GSFC", "MOTHERSON", "DUROPLY", \
#     #                "CTL", "APCL", "IRMENERGY", "SHARDACROP", "GILLETTE", "UDAYJEW", "AHLEAST", "SOFTSOL" ]  # Add your stock list here
#     # stock_names = ["RELIANCE"]  # Add your stock list here
#     logger.info(f"Starting processing for {len(stock_names)} stocks")
    
#     try:
#         await process_stock_announcements(stock_names)
#     except Exception as e:
#         logger.error(f"Main error: {e}")

#     print(f"Time taken: {time.time()-start}")
    
#     logger.info("Processing completed")

# if __name__ == "__main__":
#     asyncio.run(main())