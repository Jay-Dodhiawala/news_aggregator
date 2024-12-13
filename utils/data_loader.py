from .supabase_client import supabase_client

# Global variables to hold company names for both exchanges
bse_company_names = []
nse_company_names = []

def get_all_securities() -> list:
    """
    Fetch all securities data from the database.

    Returns:
        list: A list of dictionaries containing security data.
    """
    limit = 1000
    total_count = 3000  # total number of records you want to fetch
    securities = []

    # Loop through the range of records you want to fetch
    for start in range(0, total_count, limit):
        end = start + limit - 1  # Calculate the end index
        response = supabase_client.table('securities') \
            .select('security_name, exchange') \
            .range(start, end) \
            .execute()
        securities.extend(response.data)

    return securities

def filter_company_names(securities: list) -> tuple:
    """
    Filter the securities list into BSE and NSE company names.

    Parameters:
        securities (list): List of dictionaries containing security data

    Returns:
        tuple: (bse_names, nse_names)
    """
    bse_names = [sec['security_name'] for sec in securities if sec['exchange'] == 'BSE' and sec['security_name']]
    nse_names = [sec['security_name'] for sec in securities if sec['exchange'] == 'NSE' and sec['security_name']]
    
    return bse_names, nse_names

def initialize_company_names():
    """
    Initialize both global variables with BSE and NSE company names.
    This function is called once when the server starts.
    """
    global bse_company_names, nse_company_names
    
    # Get all securities in one query
    all_securities = get_all_securities()
    
    # Filter the securities into respective lists
    bse_company_names, nse_company_names = filter_company_names(all_securities)

# Utility functions for accessing the data
def get_bse_companies() -> list:
    """
    Get the list of BSE companies.
    
    Returns:
        list: List of BSE company names
    """
    return bse_company_names

def get_nse_companies() -> list:
    """
    Get the list of NSE companies.
    
    Returns:
        list: List of NSE company names
    """
    return nse_company_names

def get_company_counts() -> tuple:
    """
    Returns the count of companies in both exchanges.
    Useful for debugging and verification.
    
    Returns:
        tuple: (bse_count, nse_count)
    """
    return len(bse_company_names), len(nse_company_names)

# Test function to print stats
def print_stats():
    """
    Print statistics about the loaded company data.
    Useful for debugging and verification.
    """
    bse_count, nse_count = get_company_counts()
    print(f"Total companies loaded: {bse_count + nse_count}")
    print(f"BSE Companies: {bse_count}")
    print(f"NSE Companies: {nse_count}")