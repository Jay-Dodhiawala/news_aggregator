import requests
import json
import os
import sys
import asyncio
import hashlib
from typing import Tuple

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.database.retrival_utils import get_document_ids_from_name
from utils.supabase_client import supabase_client
from bse_data_collection import get_all_data_from_name
from dotenv import load_dotenv

load_dotenv()

class IpoData:
    BASE_URL = "https://stock.indianapi.in"

    def __init__(self):
        self.header = {
            "X-API-Key": os.getenv("INDIAN_STOCK_MARKET_API_KEY")
        }
        self.full_url = f"{self.BASE_URL}/ipo"
        self.supabase = supabase_client

    def _fetch_data(self) -> dict:
        """Fetch data from the API"""
        try:
            response = requests.get(self.full_url, headers=self.header)
            if response.status_code == 200:
                return response.json()
            return {}
        except Exception as e:
            print(f"Error fetching data: {e}")
            return {}
        
    def _upload_data(self, data: dict):
        """Upload data to the database"""
        # Define the file path in storage
        blob_file_path = f"test_ipo/ipo.txt"
        data_str = json.dumps(data)
        try:
            existing_files = self.supabase.storage.from_("pdf_chunks").list("test_ipo")
            file_exists = any(file['name'] == f"ipo.txt" for file in existing_files)
            if file_exists:
                response = self.supabase.storage.from_("pdf_chunks").update(
                    blob_file_path,
                    data_str.encode('utf-8'),
                    {
                        'contentType': 'text/plain'
                    }
                )
                print(f"File updated successfully for STOCK_PRICE_SHOCKERS")
            else:
                # Upload new file
                result = self.supabase.storage.from_("pdf_chunks").upload(
                    blob_file_path,
                    data_str.encode('utf-8'),
                    {
                        'contentType': 'text/plain'
                    }
                )
                print(f"File uploaded successfully for STOCK_PRICE_SHOCKERS")
        except Exception as e:
            print(f"Error uploading data: {e}")

# test = IpoData()
# data = test._fetch_data()
# print(len(data))
# test._upload_data(data)
