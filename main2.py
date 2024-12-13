from data_collection.new_announcement import NewAnnouncement
import time

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

test = NewAnnouncement()

async def process_stock_announcements(stock_names: list):
    """Process announcements for multiple stocks"""
    start_time = datetime.now()
    print(f"Starting processing at {start_time}")
    
    checker = await NewAnnouncement.create()
    total_stocks = len(stock_names)
    
    try:
        for idx, stock in enumerate(stock_names, 1):
            try:
                print(f"Processing stock {idx}/{total_stocks}: {stock}")
                success, message = await checker.check_and_update_announcements(stock)
                print(f"Stock: {stock} - Status: {success}, Message: {message}")
                
                # Add progress information
                elapsed_time = datetime.now() - start_time
                avg_time_per_stock = elapsed_time / idx
                remaining_stocks = total_stocks - idx
                estimated_time_remaining = avg_time_per_stock * remaining_stocks
                
                print(f"Progress: {idx}/{total_stocks} stocks processed")
                print(f"Estimated time remaining: {estimated_time_remaining}")
                
                await asyncio.sleep(1)  # Prevent rate limiting
                
            except Exception as e:
                print(f"Error processing {stock}: {e}")
    finally:
        await checker.close_connection()

async def main():
    start = time.time()
    # Get list of stock IDs
    # stock_names = get_bse_ids()  # Use your actual stock list
    # stock_names = ["PURPLEFIN", "ARIES", "NATIONSTD", "BATLIBOI", "GSFC", "MOTHERSON", "DUROPLY", \
    #                "CTL", "APCL", "IRMENERGY", "SHARDACROP", "GILLETTE", "UDAYJEW", "AHLEAST", "SOFTSOL" ]  # Add your stock list here
    stock_names = ["PNCINFRA"]  # Add your stock list here
    print(f"Starting processing for {len(stock_names)} stocks")
    
    try:
        await process_stock_announcements(stock_names)
    except Exception as e:
        print(f"Main error: {e}")

    print(f"Time taken: {time.time()-start}")
    
    print("Processing completed")

if __name__ == "__main__":
    asyncio.run(main())