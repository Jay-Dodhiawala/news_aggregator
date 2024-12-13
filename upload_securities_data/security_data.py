import io
import os
import sys
from typing import List, Dict
import pandas as pd
import requests
import logging
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.supabase_client import supabase_client
from dotenv import load_dotenv

load_dotenv()

BSE_URL = "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?Group=&Scripcode=&industry=&segment=Equity&status=Active"
NSE_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
INDUSTRY_URL = "https://stock.indianapi.in/stock"

def _fetch_bse_securities() -> List[Dict]:
    """
    Fetch all BSE securities from the API
    """
    headers = {
        "referer": "https://www.bseindia.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
    }
    
    try:
        response = requests.get(BSE_URL, headers=headers)
        return response.json()
    except Exception as e:
        print(f"Error fetching BSE data: {e}")
        # logging.error(f"Error fetching BSE data: {e}")
        return []
    
def _fetch_nse_securities() -> str:
    """
    Fetch all NSE securities
    """
    headers = {
    "referer": "https://www.nseindia.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
}
    
    try:
        # Now fetch the securities data
        response = requests.get(NSE_URL, headers=headers)
        return response.content.decode('utf-8')
    except Exception as e:
        print(f"Error fetching NSE data: {e}")
        # logging.error(f"Error fetching NSE data: {e}")
        return []

def _transform_bse_data(data: List[Dict]) -> pd.DataFrame:
    """
    Transform BSE API data to match our database schema
    """
    try:
        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Convert Mktcap to float
        df['Mktcap'] = pd.to_numeric(df['Mktcap'], errors='coerce')

        # Filter by market cap
        filtered_df = df[df['Mktcap'] >= 200].copy()

        # Sort by market cap (descending)
        filtered_df = filtered_df.sort_values('Mktcap', ascending=False)

        # Select and rename columns for better readability
        selected_columns = {
            'SCRIP_CD': 'security_code',
            'scrip_id': 'security_id',
            'Scrip_Name': 'security_name',
            'INDUSTRY': 'industry',
            'ISIN_NUMBER': 'isin_no'
        }
        result_df = filtered_df[selected_columns.keys()].rename(columns=selected_columns)
        result_df['exchange'] = 'BSE'
        return result_df
    except Exception as e:
        print(f"Error transforming BSE data: {e}")
        # logging.error(f"Error transforming BSE data: {e}")
        return pd.DataFrame()
    
def _transform_nse_data(data: str) -> pd.DataFrame:
    """
    Transform NSE API data to match our database schema
    """
    try:
        df = pd.read_csv(io.StringIO(data))
        # create a column for security code which is the same as symbol
        df['security_code'] = df['SYMBOL']
        # Select and rename columns for better readability
        selected_columns = {
            'security_code': 'security_code',
            'SYMBOL': 'security_id',
            'NAME OF COMPANY': 'security_name',
            ' ISIN NUMBER': 'isin_no'
        }
        result_df = df[selected_columns.keys()].rename(columns=selected_columns)
        result_df['exchange'] = 'NSE'
        return result_df
    except Exception as e:
        print(f"Error transforming NSE data: {e}")
        # logging.error(f"Error transforming NSE data: {e}")
        return pd.DataFrame()

def _merge_tranformed_data(bse_df: pd.DataFrame, nse_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge transformed BSE and NSE data
    """
    try:
        # Find the rows that are not in result_df
        print(f"bse_df: {bse_df.shape}, nse_df: {nse_df.shape}")
        rows_to_append = nse_df.loc[~nse_df['security_id'].isin(bse_df['security_id'])]
        # Append the rows to bse_df
        merged_df = bse_df.merge(rows_to_append, how='outer' )
        return merged_df
    except Exception as e:
        print(f"Error merging transformed data: {e}")
        # logging.error(f"Error merging transformed data: {e}")
        return pd.DataFrame()

def _get_existing_security_ids() -> set:
    """
    Get all existing security_ids from the database
    """
    try:
        limit = 1000
        existing_ids = set()
        
        # First get the total count
        count_response = supabase_client.table('securities').select('*', count='exact').limit(1).execute()
        total_count = count_response.count if count_response.count else 0
        
        # print(f"Total records in database: {total_count}")
        
        # Loop through all pages
        for start in range(0, total_count, limit):
            end = start + limit - 1  # Calculate the end index
            try:
                response = supabase_client.table('securities') \
                    .select('security_id') \
                    .range(start, end) \
                    .execute()
                
                batch_ids = {record['security_id'] for record in response.data}
                existing_ids.update(batch_ids)
                
                # print(f"Fetched {len(batch_ids)} security IDs from range {start}-{end}")
                
            except Exception as e:
                print(f"Error fetching range {start}-{end}: {e}")
                continue
        
        # print(f"Total unique security IDs fetched: {len(existing_ids)}")
        return existing_ids
        
    except Exception as e:
        print(f"Error fetching existing security_ids: {e}")
        import traceback
        print("Traceback:", traceback.format_exc())
        return set()
    
def _insert_security_batch(df: pd.DataFrame, batch_size: int = 100):
    """
    Insert securities in batches, skipping individual duplicates instead of whole batches
    """
    try:
        # Get existing security_ids from database (now with pagination)
        existing_ids = _get_existing_security_ids()
        
        # Get new security_ids from dataframe
        new_ids = set(df['security_id']) - existing_ids
        
        if not new_ids:
            print("No new securities to insert")
            return
        
        # Filter dataframe to only include new securities
        new_securities = df[df['security_id'].isin(new_ids)]
        
        # Remove industry column for initial insert
        insert_df = new_securities.drop(columns=['industry'])
        
        print(f"Found {len(new_ids)} new securities to insert")
        
        # Process in batches
        total_rows = len(insert_df)
        successful_inserts = 0
        
        for i in range(0, total_rows, batch_size):
            batch_df = insert_df.iloc[i:i + batch_size]
            try:
                response = supabase_client.table('securities').insert(batch_df.to_dict('records')).execute()
                successful_inserts += len(batch_df)
                print(f"Inserted batch {i//batch_size + 1}: {len(batch_df)} records "
                      f"({successful_inserts}/{total_rows} total)")
            except Exception as e:
                print(f"Error inserting batch {i//batch_size + 1}: {e}")
            
        print(f"\nInsertion complete: "
              f"\n- Total new securities: {total_rows}"
              f"\n- Successfully inserted: {successful_inserts}")
            
    except Exception as e:
        print(f"Error in batch insert process: {e}")
        import traceback
        print("Traceback:", traceback.format_exc())

def _get_industry(security_id: str) -> str:
    """
    Get industry for a security fom Indian API
    """
    try:
        querystring = {"name":security_id}
        headers = {"X-Api-Key": os.getenv("INDIAN_STOCK_MARKET_API_KEY")}
        response = requests.get(INDUSTRY_URL, headers=headers, params=querystring).json()
        return response['industry']
    except Exception as e:
        print(f"Error fetching industry for {security_id}: {e}")
        # logging.error(f"Error fetching industry for {security_id}: {e}")
        return ""

def _update_security_industry():
    """
    Update industry information for securities that don't have it
    """
    try:
        # Get securities without industry
        response = supabase_client.table('securities')\
            .select('security_id, exchange')\
            .is_('industry', 'null')\
            .execute()
        
        securities_without_industry = response.data
        
        if not securities_without_industry:
            print("No securities need industry update")
            # logging.info("No securities need industry update")
            return
        
        # Update each security's industry
        for security in securities_without_industry:
            security_id = security['security_id']
            exchange = security['exchange']
            try:
                industry = _get_industry(security_id) 
                supabase_client.table('securities')\
                    .update({'industry': industry})\
                    .eq('security_id', security_id)\
                    .execute()
                print(f"Updated industry for {security_id} ({exchange})")
                # logging.info(f"Updated industry for {security_id} ({exchange})")
            except Exception as e:
                print(f"Error updating industry for {security_id} ({exchange}): {e}")
                # logging.error(f"Error updating industry for {security_id} ({exchange}): {e}")
                
    except Exception as e:
        print(f"Error in update_security_industry: {e}")
        # logging.error(f"Error in update_security_industry: {e}")

def securities_run():
    start = datetime.now()
    bse_data = _fetch_bse_securities()
    nse_data = _fetch_nse_securities()

    if bse_data:
        bse_df = _transform_bse_data(bse_data)
    else:
        print("No BSE data to transform")

    if nse_data:
        nse_df = _transform_nse_data(nse_data)
    else:
        print("No NSE data to transform")

    if bse_df.empty and nse_df.empty:
        print("No data to merge")
        return
    
    data = _merge_tranformed_data(bse_df, nse_df)

    _insert_security_batch(data)

    _update_security_industry()

    print(f"Security data upload complete in {datetime.now() - start}")


if __name__ == "__main__":
    securities_run()
