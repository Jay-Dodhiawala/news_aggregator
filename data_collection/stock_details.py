import websockets
import json
import os
import sys
import asyncio
import hashlib
from typing import Tuple

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.database.retrival_utils import get_document_ids_from_name, get_bse_ids, list_all_storage_files
from utils.supabase_client import supabase_client
from bse_data_collection import get_all_data_from_name
from dotenv import load_dotenv

load_dotenv()

class StockDetails:
    BASE_URL = "wss://analyst-ws.indianapi.in"
    

    def __init__(self):
        self.header = {
            "X-API-Key": os.getenv("INDIAN_STOCK_MARKET_API_KEY")
        }
        self.full_url = f"{self.BASE_URL}/ws/stock"
        self.websocket = None
        self.supabase = supabase_client

        self.storage_name = "pdf_chunks"
        self.folder_name = "test_details"
        self.existing_files = list_all_storage_files(self.storage_name, self.folder_name)

    @classmethod
    async def create(cls):
        self = cls()
        self.websocket = await websockets.connect(self.full_url, extra_headers=self.header)
        return self

    async def close_connection(self):
        if self.websocket:
            await self.websocket.close(code=1000, reason="Closing connection")

    def _generate_hash(self, content: str) -> str:
        """Generate SHA-256 hash of the content"""
        return hashlib.sha256(content.encode('utf-8')).hexdigest()

    def _check_existing_hash(self, content_hash: str) -> bool:
        """Check if the hash already exists in the database"""
        try:
            response = self.supabase.table('stock_details').select('hash').eq('hash', content_hash).execute()
            return len(response.data) > 0
        except Exception as e:
            print(f"Error checking hash existence: {e}")
            return False

    async def _insert_stock_details(self, stock_name: str, content: str, content_hash: str) -> bool:
        """Insert new stock details into the database and storage"""
        
        try:
            # Define the file path in storage
            blob_file_path = f"{self.folder_name}/{stock_name}.txt"
            
            # Check if file exists in storage
            try:
               
                # existing_files = self.supabase.storage.from_(storage_name).list(folder_name)
                # print(f"Existing files: the length is {len(existing_files)}")
                file_exists = any(file['name'] == f"{stock_name}.txt" for file in self.existing_files)
                print(f"File exists: {file_exists}")
                if file_exists:
                    # Update existing file
                    result = self.supabase.storage.from_("pdf_chunks").update(
                        blob_file_path,
                        content.encode('utf-8'),
                        {
                            'contentType': 'text/plain'
                        }
                    )
                    print(f"File updated successfully for {stock_name}")
                else:
                    # Upload new file
                    result = self.supabase.storage.from_("pdf_chunks").upload(
                        blob_file_path,
                        content.encode('utf-8'),
                        {
                            'contentType': 'text/plain'
                        }
                    )
                    print(f"File uploaded successfully for {stock_name}")

            except Exception as storage_error:
                print(f"Storage operation failed: {storage_error}")
                return False

            # Update database record
            data = {
                "stock_name": stock_name.upper(),
                "hash": content_hash,
                "content": blob_file_path
            }

            response = self.supabase.table('stock_details').upsert(data).execute()
            return len(response.data) > 0
            
        except Exception as e:
            print(f"Error in _insert_stock_details: {e}")
            return False

    async def check_and_update_details(self, stock_name: str) -> Tuple[bool, str]:
        """
        Check for new announcements and update if new hash is found
        
        Returns:
        Tuple[bool, str]: (success, message)
        """
        try:
            # Send request for announcements
            message = f'{{"name": "{stock_name}"}}'
            await self.websocket.send(message)
            
            # Get response
            response = await self.websocket.recv()
            # Generate hash of the raw response
            content_hash = self._generate_hash(response)
            print(f"Content hash: {content_hash}")
            # Check if this hash already exists
            if self._check_existing_hash(content_hash):
                return False, "Details already exist"
            

            # If hash doesn't exist, insert the new announcement
            success = await self._insert_stock_details(stock_name, response, content_hash)
            
            if success:
                return True, "New Details added successfully"
            else:
                return False, "Failed to add details"
                
        except Exception as e:
            return False, f"Error processing stock details: {str(e)}"

ids = get_bse_ids()
# Example usage:
async def main():
    checker = await StockDetails.create()
    try:
        # success, message = await checker.check_and_update_details('OIL')
        # print(f"Status: {success}, Message: {message}")
        # for id in ids:
        #     print('id:', id)
        #     success, message = await checker.check_and_update_details(id)
        #     print(f"Status: {success}, Message: {message}")
        print('ids:', ids)
    finally:
        await checker.close_connection()

if __name__ == "__main__":
    asyncio.run(main())