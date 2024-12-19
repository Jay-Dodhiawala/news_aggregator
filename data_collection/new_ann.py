# import requests
# import json
# import os
# import sys
# import time
# import asyncio
# from datetime import datetime, timedelta
# import logging
# from typing import List, Dict, Optional
# from dataclasses import dataclass
# import hashlib

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# from utils.supabase_client import supabase_client
# from data_collection.bse_data_collection import get_all_data_from_name
# from utils.database.retrival_utils import get_company_name
# from utils.queue_processor import AnnouncementQueue, QueueItem

# # Set up logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger(__name__)

# announcement_queue = AnnouncementQueue()

# class NewAnnouncementChecker:
#     BASE_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
#     ANNOUNCEMET_HASH = ""
    
#     def __init__(self):
#         self.supabase = supabase_client
#         self.headers = {
#             "referer": "https://www.bseindia.com/",
#             "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
#         }
#         self.queue = AnnouncementQueue()

#     def _get_date_params(self) -> tuple:
#         """Get today's date parameters in required format"""
#         today = datetime.now()
#         return (today.strftime("%Y%m%d"), today.strftime("%Y%m%d"))

#     def _generate_hash(self, content: str) -> str:
#         """Generate hash for announcement content"""
#         return hashlib.sha256(content.encode('utf-8')).hexdigest()

#     def _check_existing_announcement(self, company_code: str, announcement_date: str) -> bool:
#         """Check if announcement already exists in database"""
#         try:
#             response = self.supabase.table('documents')\
#                 .select('id')\
#                 .eq('company_code', company_code)\
#                 .eq('upload_date', announcement_date)\
#                 .execute()
#             return len(response.data) > 0
#         except Exception as e:
#             logger.error(f"Error checking existing announcement: {e}")
#             return False

#     def fetch_new_announcements(self) -> List[Dict]:
#         """Fetch new announcements from BSE API"""
#         from_date, to_date = self._get_date_params()
        
#         params = {
#             "pageno": "1",
#             "strCat": "-1",  # All categories
#             "strPrevDate": from_date,
#             "strScrip": "",  # All scripts
#             "strSearch": "P",
#             "strToDate": to_date,
#             "strType": "C",
#             "subcategory": "-1"
#         }

#         try:
#             response = requests.get(
#                 self.BASE_URL,
#                 params=params,
#                 headers=self.headers,
#                 timeout=30
#             )
#             response.raise_for_status()
            
#             data = response.json()
#             if "Table" not in data:
#                 logger.warning("No announcements found in response")
#                 return []
                
#             return data["Table"]
            
#         except requests.exceptions.RequestException as e:
#             logger.error(f"Error fetching announcements: {e}")
#             return []

#     async def queue_announcements(self) -> None:
#         """Add new announcements to the processing queue"""
#         logger.info("Starting announcement check")
#         start_time = time.time()
        
#         announcements = self.fetch_new_announcements()
#         queued_count = 0

#         announcements_hash = self._generate_hash(json.dumps(announcements))

#         if announcements_hash == self.ANNOUNCEMET_HASH:
#             logger.info("No new announcements found")
#         else:
        
#             for announcement in announcements:
#                 try:
#                     company_code = announcement.get("SCRIP_CD")
#                     if not company_code:
#                         continue
                        
#                     upload_date = announcement.get("DT_TM", "").split('T')[0]
                    
#                     # Skip if announcement already processed
#                     if self._check_existing_announcement(company_code, upload_date):
#                         logger.info(f"Announcement for {company_code} on {upload_date} already exists")
#                         continue
                    
#                     # Get company name and queue for processing
#                     company_name = get_company_name(str(company_code))
#                     if company_name:
#                         # Calculate priority based on market cap or other criteria
#                         priority = 1  # Default priority
                        
#                         # Add to queue
#                         added = await self.queue.add_item(company_name, priority)
#                         if added:
#                             queued_count += 1
#                             logger.info(f"Queued new announcement for {company_name}")
                        
#                 except Exception as e:
#                     logger.error(f"Error queuing announcement: {e}")
#                     continue

#         execution_time = time.time() - start_time
#         logger.info(f"Announcement check completed. Queued {queued_count} new announcements in {execution_time:.2f} seconds")

# async def run_queue_processor(batch_size: int = 5):
#     """Run the queue processor"""
#     logger.info("Starting queue processor")
#     while True:
#         try:
#             batch = []
#             # Get items from queue up to batch_size
#             for _ in range(batch_size):
#                 item = await announcement_queue.get_next_item()
#                 if item:
#                     batch.append(item)
#                 if len(batch) >= batch_size:
#                     break

#             if not batch:
#                 await asyncio.sleep(1)  # Wait if queue is empty
#                 continue

#             # Process batch concurrently
#             tasks = []
#             for item in batch:
#                 task = asyncio.create_task(process_stock(item))
#                 tasks.append(task)
            
#             await asyncio.gather(*tasks)

#         except Exception as e:
#             logger.error(f"Error in queue processor: {e}")
#             await asyncio.sleep(1)

# async def process_stock(item: QueueItem):
#     """Process a single stock from the queue"""
#     try:
#         logger.info(f"Processing {item.stock_name}")
#         get_all_data_from_name(item.stock_name)
#         logger.info(f"Completed processing {item.stock_name}")
#     except Exception as e:
#         logger.error(f"Error processing {item.stock_name}: {e}")
#     finally:
#         announcement_queue.mark_complete(item.stock_name)

# async def main():
#     """Main function to run both announcement checker and queue processor"""
#     checker = NewAnnouncementChecker()
    
#     # Create tasks for announcement checking and queue processing
#     announcement_task = asyncio.create_task(run_announcement_checker(checker))
#     processor_task = asyncio.create_task(run_queue_processor(batch_size=5))
    
#     try:
#         # Run both tasks concurrently
#         await asyncio.gather(announcement_task, processor_task)
#     except KeyboardInterrupt:
#         logger.info("Shutting down...")
#         checker.queue.stop()

# async def run_announcement_checker(checker: NewAnnouncementChecker):
#     """Run the announcement checker periodically"""
#     while True:
#         try:
#             await checker.queue_announcements()
#             # Wait for 5 minutes before next check
#             await asyncio.sleep(300)
#         except Exception as e:
#             logger.error(f"Error in announcement checker: {e}")
#             await asyncio.sleep(60)  # Wait a minute before retry

# if __name__ == "__main__":
#     # Run everything
#     asyncio.run(main())

import requests
import json
import os
import sys
import time
import asyncio
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass
import hashlib
import openai

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.supabase_client import supabase_client
from data_collection.bse_data_collection import get_all_data_from_name
# from data_collection.bse_data_collection import get_all_data_from_name, download_pdf_from_link, generate_summary
from utils.database.retrival_utils import get_company_name
from utils.queue_processor import AnnouncementQueue, QueueItem

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NewAnnouncementChecker:
    BASE_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
    ANNOUNCEMET_HASH = ""
    
    def __init__(self):
        self.supabase = supabase_client
        self.headers = {
            "referer": "https://www.bseindia.com/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
        }
        self.queue = AnnouncementQueue()

    def _get_date_params(self) -> tuple:
        """Get today's date parameters in required format"""
        today = datetime.now()
        return (today.strftime("%Y%m%d"), today.strftime("%Y%m%d"))

    def _generate_hash(self, content: str) -> str:
        """Generate hash for announcement content"""
        return hashlib.sha256(content.encode('utf-8')).hexdigest()

    def _check_existing_announcement(self, pdf_name: str) -> bool:
        """Check if announcement already exists in database"""
        try:
            response = supabase_client.table('recent_announcements_2')\
                .select('content')\
                .eq('content', pdf_name)\
                .execute()
            return len(response.data) > 0
        except Exception as e:
            print(f"Error checking existing announcement: {e}")
            return False
        
    def remove_duplicate_scrips(self, data_list):
        seen_scrip_codes = set()
        unique_data = []
        
        for entry in data_list:
            scrip_code = entry.get('SCRIP_CD')
            if scrip_code not in seen_scrip_codes:
                seen_scrip_codes.add(scrip_code)
                unique_data.append(entry)
        
        return unique_data
        
    async def _store_announcement(self, stock_name: str, content: str, title:str) -> bool:
        # async def _store_announcement(self, stock_name: str, content: str, summary: str, content_hash: str) -> bool:
        """Store announcement in recent_announcements_2 table"""
        try:
            data = {
                "stock_name": stock_name.upper(),
                "content": content,
                "title": title
                # "summary": summary,
                # "hash": content_hash
            }
            
            response = self.supabase.table('recent_announcements_2')\
                .upsert(data)\
                .execute()
                
            return len(response.data) > 0
        except Exception as e:
            print(f"Error storing announcement: {e}")
            return False

    def fetch_new_announcements(self) -> List[Dict]:
        """Fetch new announcements from BSE API"""
        from_date, to_date = self._get_date_params()
        
        params = {
            "pageno": "1",
            "strCat": "-1",  # All categories
            "strPrevDate": from_date,
            "strScrip": "",  # All scripts
            "strSearch": "P",
            "strToDate": to_date,
            "strType": "C",
            "subcategory": "-1"
        }

        try:
            response = requests.get(
                self.BASE_URL,
                params=params,
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            if "Table" not in data:
                logger.warning("No announcements found in response")
                return []
                
            return self.remove_duplicate_scrips(data["Table"])
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching announcements: {e}")
            return []

    # async def process_announcement_content(self, announcement: Dict) -> tuple:
        """Process announcement content and generate summary"""
        try:
            pdf_name = announcement.get("ATTACHMENTNAME")
            # if not pdf_name:
            #     return None, None
                
            # # Construct PDF URL
            # pdf_url = f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{pdf_name}"
            
            # # Download and extract text from PDF
            # pdf_text = download_pdf_from_link(pdf_url)
            # if not pdf_text:
            #     return None, None
                
            # # Generate summary using your existing function
            # summary = generate_summary(pdf_text)
            
            # return pdf_text, summary
            return pdf_name
            
        except Exception as e:
            logger.error(f"Error processing announcement content: {e}")
            return None, None
        
    async def process_announcement_content(self, announcement: Dict) -> tuple:
        """Process announcement content and generate summary"""
        try:
            pdf_name = announcement.get("ATTACHMENTNAME")
            title = announcement.get("MORE")
            if title == "":
                title = announcement.get("HEADLINE")
            return pdf_name, title
            
        except Exception as e:
            print(f"Error processing announcement content: {e}")
            return None, None

    async def queue_announcements(self) -> None:
        """Add new announcements to the processing queue"""
        print("Starting announcement check")
        start_time = time.time()
        
        announcements = self.fetch_new_announcements()
        queued_count = 0
        
        for announcement in announcements:
            try:
                company_code = announcement.get("SCRIP_CD")
                if not company_code:
                    continue
                
                # Get company name
                company_name = get_company_name(str(company_code))
                if not company_name:
                    continue

                # Process announcement content
                content, title = await self.process_announcement_content(announcement)
                # if not content:
                #     continue

                # # Generate hash and check if already processed
                # content_hash = _generate_hash(content)
                print(f"get_company_name: {company_name} - {content}")
                if self._check_existing_announcement(content):
                    print(f"Announcement for {company_name} already exists")
                    continue

                # Store in recent_announcements_2 table
                stored = await self._store_announcement(
                    company_name,
                    content,
                    title
                    # summary,
                )
                
                if stored:
                    # Add to processing queue
                    priority = 1  # Default priority
                    added = await self.queue.add_item(company_name, priority)
                    if added:
                        queued_count += 1
                        print(f"Queued new announcement for {company_name}")
                    
            except Exception as e:
                print(f"Error queuing announcement: {e}")
                continue

        execution_time = time.time() - start_time
        print(f"Announcement check completed. Queued {queued_count} new announcements in {execution_time:.2f} seconds")


async def run_queue_processor(queue: AnnouncementQueue, batch_size: int = 5):
    """Run the queue processor"""
    logger.info("Starting queue processor")
    while True:
        try:
            batch = []
            # Get items from queue up to batch_size
            for _ in range(batch_size):
                item = await queue.get_next_item()
                if item:
                    batch.append(item)
                if len(batch) >= batch_size:
                    break

            if not batch:
                await asyncio.sleep(1)  # Wait if queue is empty
                continue

            # Process batch concurrently
            tasks = []
            for item in batch:
                task = asyncio.create_task(process_stock(item, queue))
                tasks.append(task)
            
            await asyncio.gather(*tasks)

        except Exception as e:
            logger.error(f"Error in queue processor: {e}")
            await asyncio.sleep(1)

async def process_stock(item: QueueItem, queue: AnnouncementQueue):
    """Process a single stock from the queue"""
    try:
        logger.info(f"Processing {item.stock_name}")
        get_all_data_from_name(item.stock_name)
        logger.info(f"Completed processing {item.stock_name}")
    except Exception as e:
        logger.error(f"Error processing {item.stock_name}: {e}")
    finally:
        queue.mark_complete(item.stock_name)

async def main():
    """Main function to run both announcement checker and queue processor"""
    checker = NewAnnouncementChecker()
    
    # Create tasks for announcement checking and queue processing
    announcement_task = asyncio.create_task(run_announcement_checker(checker))
    processor_task = asyncio.create_task(run_queue_processor(checker.queue, batch_size=5))
    
    try:
        # Run both tasks concurrently
        await asyncio.gather(announcement_task, processor_task)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        checker.queue.stop()

async def run_announcement_checker(checker: NewAnnouncementChecker):
    """Run the announcement checker periodically"""
    while True:
        try:
            await checker.queue_announcements()
            # Wait for 3 minutes before next check
            await asyncio.sleep(61)
        except Exception as e:
            logger.error(f"Error in announcement checker: {e}")
            await asyncio.sleep(60)  # Wait a minute before retry

if __name__ == "__main__":
    # Run everything
    asyncio.run(main())