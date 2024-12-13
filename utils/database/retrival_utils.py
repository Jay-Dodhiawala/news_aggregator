import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.supabase_client import supabase_client as supabase

def get_company_name(security_code:str):
    response = supabase.table('securities').select('security_id').eq('security_code', str(security_code).upper()).execute()
    if response.data:
        return response.data[0]['security_id']
    return None

def get_company_full_name(security_id:str):
    response = supabase.table('securities').select('security_name').eq('security_id', security_id.upper()).execute()
    if response.data:
        return response.data[0]['security_name']
    return None

def get_security_code(company_name):
    response = supabase.table('securities').select('security_code').eq('security_id', company_name.upper()).execute()
    if response.data:
        return response.data[0]['security_code']
    return None

def get_document_ids(security_code):
    response = supabase.table('documents').select('id').eq('company_code', security_code).execute()
    return [doc['id'] for doc in response.data]

def get_document_ids_from_name(comapany_name):
    security_code = get_security_code(comapany_name)
    response = supabase.table('documents').select('pdf_name').eq('company_code', security_code).execute()
    return [doc['pdf_name'] for doc in response.data]

def get_all_securities_codes():

    limit = 1000
    total_count = 3000  # total number of records you want to fetch
    codes = []

    # Loop through the range of records you want to fetch
    for start in range(0, total_count, limit):
        end = start + limit - 1  # Calculate the end index
        response = supabase.table('securities').select('security_code').range(start, end).execute()
        # Add the fetched names to the list
        codes.extend(sec['security_code'] for sec in response.data)

    return codes

    # response = supabase.table('securities').select('security_code').execute()
    # return [sec['security_code'] for sec in response.data]

def get_all_securities_ids():

    limit = 1000
    total_count = 3000  # total number of records you want to fetch
    codes = []

    # Loop through the range of records you want to fetch
    for start in range(0, total_count, limit):
        end = start + limit - 1  # Calculate the end index
        response = supabase.table('securities').select('security_id').range(start, end).execute()
        # Add the fetched names to the list
        codes.extend(sec['security_id'] for sec in response.data)

    return codes

def get_bse_ids():

    limit = 1000
    total_count = 3000  # total number of records you want to fetch
    ids = []

    # Loop through the range of records you want to fetch
    for start in range(0, total_count, limit):
        end = start + limit - 1  # Calculate the end index
        response = supabase.table('securities').select('security_id').eq('exchange', 'BSE').range(start, end).execute()
        # Add the fetched names to the list
        ids.extend(sec['security_id'] for sec in response.data)

    return ids


    response = supabase.table('securities').select('security_id').eq('exchange', 'BSE').execute()
    return [sec['security_id'] for sec in response.data]

def list_all_storage_files(storage_name: str, folder_name: str) -> list:
    """
    List all files from a Supabase storage bucket with pagination
    
    Args:
        storage_name (str): Name of the storage bucket
        folder_name (str): Name of the folder within the bucket
        
    Returns:
        list: Complete list of all files
    """
    all_files = []
    offset = 0
    limit = 100  # Supabase's default limit
    
    while True:
        try:
            # Get batch of files with offset
            files = supabase.storage.from_(storage_name)\
                .list(folder_name, options={
                    'limit': limit,
                    'offset': offset,
                    'sortBy': {'column': 'name', 'order': 'asc'}
                })
            
            # If no files returned, we've reached the end
            if not files:
                break
                
            # Add files to our complete list
            all_files.extend(files)
            
            # If we got less than the limit, we've reached the end
            if len(files) < limit:
                break
                
            # Increment offset for next batch
            offset += limit
            
            # Optional: Print progress
            print(f"Retrieved {len(all_files)} files so far...")
            
        except Exception as e:
            print(f"Error retrieving files at offset {offset}: {str(e)}")
            break
    
    print(f"Total files retrieved: {len(all_files)}")
    return all_files




