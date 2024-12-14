# import websockets
# import json
# import os
# import sys
# import asyncio
# import time
# import hashlib
# from typing import Tuple

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# from utils.database.retrival_utils import get_document_ids_from_name
# from utils.supabase_client import supabase_client
# from bse_data_collection import get_all_data_from_name, download_pdf_from_link, generate_summary
# from dotenv import load_dotenv

# load_dotenv()

# class NewAnnouncement:
#     BASE_URL = "wss://analyst-ws.indianapi.in"
#     MAX_RETRIES = 3
#     RETRY_DELAY = 2  # seconds
#     TIMEOUT = 30  # seconds

#     def __init__(self):
#         self.header = {
#             "X-API-Key": os.getenv("INDIAN_STOCK_MARKET_API_KEY")
#         }
#         self.full_url = f"{self.BASE_URL}/ws/recent_announcements"
#         self.websocket = None
#         self.supabase = supabase_client

#     @classmethod
#     async def create(cls):
#         self = cls()
#         await self._connect()
#         return self
    
#     async def _connect(self):
#         """Establish WebSocket connection with error handling"""
#         try:
#             self.websocket = await websockets.connect(
#                 self.full_url, 
#                 extra_headers=self.header,
#                 ping_interval=None,  # Disable automatic ping
#                 close_timeout=5
#             )
#         except Exception as e:
#             print(f"Connection error: {e}")
#             raise

#     async def reconnect(self):
#         """Reconnect to the WebSocket server with backoff"""
#         if self.websocket:
#             try:
#                 await self.websocket.close()
#             except:
#                 pass
#         await asyncio.sleep(self.RETRY_DELAY)
#         await self._connect()

#     async def close_connection(self):
#         """Safely close the WebSocket connection"""
#         if self.websocket:
#             try:
#                 await self.websocket.close()
#             except:
#                 pass

#     async def _send_with_timeout(self, message: str, timeout: int = 10) -> str:
#         """Send message with timeout and handle connection issues"""
#         try:
#             await asyncio.wait_for(self.websocket.send(message), timeout=timeout)
#             response = await asyncio.wait_for(self.websocket.recv(), timeout=timeout)
#             return response
#         except asyncio.TimeoutError:
#             raise TimeoutError("Operation timed out")
#         except websockets.exceptions.ConnectionClosed:
#             raise ConnectionError("WebSocket connection closed")

#     # def __init__(self):
#     #     self.header = {
#     #         "X-API-Key": os.getenv("INDIAN_STOCK_MARKET_API_KEY")
#     #     }
#     #     self.full_url = f"{self.BASE_URL}/ws/recent_announcements"
#     #     self.websocket = None
#     #     self.supabase = supabase_client

#     # @classmethod
#     # async def create(cls):
#     #     self = cls()
#     #     self.websocket = await websockets.connect(
#     #         self.full_url, 
#     #         extra_headers=self.header,
#     #         ping_interval=20,
#     #         ping_timeout=10,
#     #         close_timeout=10
#     #     )
#     #     return self
    
#     async def reconnect(self):
#         """Reconnect to the WebSocket server"""
#         if self.websocket:
#             await self.websocket.close()
#         self.websocket = await websockets.connect(
#             self.full_url,
#             extra_headers=self.header,
#             ping_interval=20,
#             ping_timeout=10,
#             close_timeout=10
#         )

#     async def close_connection(self):
#         if self.websocket:
#             await self.websocket.close(code=1000, reason="Closing connection")

#     def _generate_hash(self, content: str) -> str:
#         """Generate SHA-256 hash of the content"""
#         return hashlib.sha256(content.encode('utf-8')).hexdigest()

#     def _check_existing_hash(self, content_hash: str) -> bool:
#         """Check if the hash already exists in the database"""
#         try:
#             response = self.supabase.table('recent_announcements').select('hash').eq('hash', content_hash).execute()
#             return len(response.data) > 0
#         except Exception as e:
#             print(f"Error checking hash existence: {e}")
#             return False


#     def _get_list_of_pdf_names(self, response: dict) -> list:
#         return [item['link'].split("=")[-1] for item in response]
    
#     def process_announcement(self, response: dict):
#         processed_announcements = []
        
#         for announcement in response:
#             pdf_link = announcement['link']
#             pdf_text = download_pdf_from_link(pdf_link)
#             summary = generate_summary(pdf_text)
            
#             processed_announcement = {
#                 "title": announcement['title'],
#                 "link": announcement['link'],
#                 "summary": summary
#             }
#             processed_announcements.append(processed_announcement)
        
#         return processed_announcements

#     async def _insert_announcement(self, stock_name: str, content: str, content_hash: str, summary:list[dict]) -> bool:
#         """Insert new announcement into the database"""
#         try:
#             data = {
#                 "stock_name": stock_name.upper(),  # Convert to uppercase to match security_id format
#                 "hash": content_hash,
#                 "content": content,
#                 "summary": summary
#             }
#             response = self.supabase.table('recent_announcements').upsert(data).execute()
#             return len(response.data) > 0
#         except Exception as e:
#             print(f"Error inserting announcement: {e}")
#             return False

#     async def check_and_update_announcements(self, stock_name: str) -> Tuple[bool, str]:
#         """Check for new announcements with improved error handling"""
#         for attempt in range(self.MAX_RETRIES):
#             try:
#                 # Prepare and send the request
#                 message = json.dumps({"stock_name": stock_name})
#                 response = await self._send_with_timeout(message)
                
#                 # Process the response
#                 content_hash = self._generate_hash(response)
#                 if self._check_existing_hash(content_hash):
#                     return False, "Announcement already exists"
                
#                 # Update data and process announcement
#                 get_all_data_from_name(stock_name)
#                 processed_announcements = self.process_announcement(json.loads(response))
                
#                 # Insert the announcement
#                 success = await self._insert_announcement(
#                     stock_name, response, content_hash, processed_announcements
#                 )
                
#                 if success:
#                     return True, "New announcement added successfully"
#                 return False, "Failed to add announcement"

#             except (TimeoutError, ConnectionError) as e:
#                 print(f"Attempt {attempt + 1}/{self.MAX_RETRIES} failed: {str(e)}")
#                 if attempt < self.MAX_RETRIES - 1:
#                     await self.reconnect()
#                 continue
                
#             except Exception as e:
#                 return False, f"Error processing announcement: {str(e)}"
        
#         return False, f"Failed after {self.MAX_RETRIES} attempts"

        

#     # async def check_and_update_announcements(self, stock_name: str) -> Tuple[bool, str]:
#     #     """
#     #     Check for new announcements and update if new hash is found
        
#     #     Returns:
#     #     Tuple[bool, str]: (success, message)
#     #     """
#     #     # for attempt in range(self.MAX_RETRIES):
#     #     try:
#     #         # Send request for announcements
#     #         message = f'{{"stock_name": "{stock_name}"}}'
#     #         await self.websocket.send(message)
            
#     #         # Get response
#     #         response = await self.websocket.recv()
            
#     #         # Generate hash of the raw response
#     #         content_hash = self._generate_hash(response)
            
#     #         # Check if this hash already exists
#     #         if self._check_existing_hash(content_hash):
#     #             return False, "Announcement already exists"
            
#     #         # update the chuicnks tables with new data
#     #         get_all_data_from_name(stock_name) # this is for document table

#     #         # Process the announcement
#     #         processed_announcements = self.process_announcement(json.loads(response))

#     #         # If hash doesn't exist, insert the new announcement
#     #         success = await self._insert_announcement(stock_name, response, content_hash, processed_announcements)
            
#     #         if success:
#     #             return True, "New announcement added successfully"
#     #         else:
#     #             return False, "Failed to add announcement"
                
#     #     except Exception as e:
#     #         return False, f"Error processing announcement: {str(e)}"

# # Example usage:
# async def main():
#     start = time.time()
#     checker = None
#     try:
#         checker = await NewAnnouncement.create()
#         ids = ['20MICRONS']
#         # ids = ['FACT', 'IREDA', 'THERMAX', 'PHOENIXLTD', 'BALKRISIND', 'MPHASIS', 'UNOMINDA', 'MRF' , 'LTTS', 'PGHH', 
#         #        'CONCOR', 'TATACOMM', 'NYKAA', 'IDFCFIRSTB', 'SUNDARMFIN', 'LLOYDSME', 'UBL', 'PAGEIND', 'FLUOROCHEM', 'CENTRALBK']
        
#         for id in ids:
#             try:
#                 success, message = await checker.check_and_update_announcements(id)
#                 print(f"Stock: {id}, Status: {success}, Message: {message}")
#             except Exception as e:
#                 print(f"Error processing {id}: {str(e)}")
#                 continue
            
#     except Exception as e:
#         print(f"Fatal error: {str(e)}")
#     finally:
#         if checker:
#             await checker.close_connection()
#     print(f"Time taken: {time.time()-start}")

# if __name__ == "__main__":
#     asyncio.run(main())















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
                processed_announcements = self.process_announcement(response_data)
                
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

    # def download_pdf_from_link(pdf_link: str, max_retries: int = 3) -> str:
    #     """
    #     Download and extract text from a PDF given its URL with improved error handling and timeouts.
        
    #     Args:
    #         pdf_link: Full URL to the PDF file
    #         max_retries: Maximum number of retry attempts
        
    #     Returns:
    #         Extracted text content from the PDF or empty string if failed
    #     """
    #     logger.info(f"Attempting to download PDF from: {pdf_link}")
        
    #     # Configure session with retries
    #     session = requests.Session()
    #     retries = Retry(
    #         total=max_retries,
    #         backoff_factor=1,
    #         status_forcelist=[500, 502, 503, 504],
    #     )
    #     adapter = HTTPAdapter(max_retries=retries)
    #     session.mount('http://', adapter)
    #     session.mount('https://', adapter)
        
    #     # Set headers
    #     headers = {
    #         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    #         'Accept': 'application/pdf,*/*',
    #         'Accept-Language': 'en-US,en;q=0.9',
    #         'Connection': 'keep-alive',
    #     }

    #     try:
    #         # Download PDF with timeout
    #         start_time = time.time()
    #         logger.info("Starting PDF download")
            
    #         response = session.get(
    #             pdf_link, 
    #             headers=headers, 
    #             timeout=(10, 30),  # (connect timeout, read timeout)
    #             allow_redirects=True,
    #             stream=True  # Stream the response
    #         )
    #         response.raise_for_status()
            
    #         # Check if the response is actually a PDF
    #         content_type = response.headers.get('content-type', '').lower()
    #         if 'pdf' not in content_type:
    #             logger.error(f"Received non-PDF content type: {content_type}")
    #             return ""

    #         # Read the response in chunks
    #         pdf_content = io.BytesIO()
    #         chunk_size = 8192  # 8KB chunks
            
    #         for chunk in response.iter_content(chunk_size=chunk_size):
    #             if chunk:
    #                 pdf_content.write(chunk)
            
    #         download_time = time.time() - start_time
    #         logger.info(f"PDF download completed in {download_time:.2f} seconds")

    #         # Extract text with timeout using ThreadPoolExecutor
    #         logger.info("Starting text extraction")
    #         start_time = time.time()
            
    #         def extract_with_timeout():
    #             try:
    #                 return extract_text(pdf_content)
    #             except Exception as e:
    #                 logger.error(f"Text extraction failed: {e}")
    #                 return ""

    #         # Use ThreadPoolExecutor with timeout
    #         with ThreadPoolExecutor() as executor:
    #             future = executor.submit(extract_with_timeout)
    #             try:
    #                 raw_text = future.result(timeout=60)  # 60 second timeout for text extraction
    #                 extraction_time = time.time() - start_time
    #                 logger.info(f"Text extraction completed in {extraction_time:.2f} seconds")
    #                 return raw_text
    #             except TimeoutError:
    #                 logger.error("Text extraction timed out")
    #                 return ""
                
    #     except requests.exceptions.Timeout:
    #         logger.error(f"Timeout downloading PDF from {pdf_link}")
    #         return ""
    #     except requests.exceptions.RequestException as e:
    #         logger.error(f"Error downloading PDF from {pdf_link}: {e}")
    #         return ""
    #     except Exception as e:
    #         logger.error(f"Unexpected error processing PDF from {pdf_link}: {e}")
    #         return ""
    #     finally:
    #         session.close()

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


async def main():
    start = time.time()
    # Get list of stock IDs
    # stock_names = get_bse_ids()  # Use your actual stock list
    # stock_names = ["PURPLEFIN", "ARIES", "NATIONSTD", "BATLIBOI", "GSFC", "MOTHERSON", "DUROPLY", \
    #                "CTL", "APCL", "IRMENERGY", "SHARDACROP", "GILLETTE", "UDAYJEW", "AHLEAST", "SOFTSOL" ]  # Add your stock list here
    stock_names = ["RELIANCE"]  # Add your stock list here
    logger.info(f"Starting processing for {len(stock_names)} stocks")
    
    try:
        await process_stock_announcements(stock_names)
    except Exception as e:
        logger.error(f"Main error: {e}")

    print(f"Time taken: {time.time()-start}")
    
    logger.info("Processing completed")

if __name__ == "__main__":
    asyncio.run(main())