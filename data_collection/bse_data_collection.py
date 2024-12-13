import requests
from datetime import datetime, timedelta
from collections import defaultdict
from pdfminer.high_level import extract_text
import io
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pdfminer.high_level import extract_text
from langchain.text_splitter import RecursiveCharacterTextSplitter
import hashlib
import uuid
from typing import Optional
from upload_securities_data.financial_report_extraction import FinancialContentExtractor
from utils.database.retrival_utils import get_company_name

# from langchain_community.embeddings import OpenAIEmbeddings
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import SupabaseVectorStore
from langchain.schema import Document
import openai

from supabase import create_client
from utils.supabase_client import create_supabase_client
from utils.database.retrival_utils import get_security_code
import os
from dotenv import load_dotenv

load_dotenv()

# Initialize your Supabase client here
# url = os.getenv("SUPABASE_URL")
# key = os.getenv("SUPABASE_KEY")
# supabase_client = create_client(url, key)

fin_extractor = FinancialContentExtractor(os.getenv("GEMINI_API_KEY"))


# def fetch_bse_data(
#     pageno: str = "1",
#     strCat: str = "Board Meeting",
#     strPrevDate: Optional[str] = None,
#     strScrip: str = "532215",
#     strSearch: str = "P",
#     strToDate: Optional[str] = None,
#     strType: str = "C",
#     subcategory: str = "-1"
# ) -> list:
#     """
#     Fetch data from the BSE API using the specified parameters.
#     Defaults are used if values are not provided by the user.
    
#     Parameters:
#     - pageno: The page number for pagination (default: "1").
#     - strCat: Category of the data, e.g., 'Board Meeting', 'AGM/EGM', 'Result' (default: 'Board Meeting').
#     - strPrevDate: Starting date of the range in YYYYMMDD format (default: "20240718").
#     - strScrip: Company Scrip Code (default: "532215").
#     - strSearch: Search type (default: "P").
#     - strToDate: End date of the range in YYYYMMDD format (default: "20241018").
#     - strType: Type of the announcement (default: "C").
#     - subcategory: Subcategory ID (default: "-1").
#     - base_url: The base URL for the API request (default: BSE announcement API URL).

#     Returns:
#     - list of information if the request is successful.
#     - Error message if the request fails.
#     """
#     category_subcategory_map = {
#         "AGM/EGM": ["AGM"],
#         "Board Meeting": ["Board Meeting", "Outcome of Board Meeting"],
#         "Corp. Action": ["Sub-division/Stock Split", "Record Date", "Dividend", "Bonds/Right Issue", "Bonus"],
#         "Result": ["Financial Results"],
#         "Company Update": [
#             "General",
#             "Acquisition",
#             "Allotment of Equity Shares",
#             "Allotment of Warrants",
#             "Award of Order / Receipt of Order",
#             "Buy back",
#             "Delisting",
#             "Joint Venture",
#             "Open Offer",
#             "Debt Securities",
#             "Credit Rating",
#             "Shareholding",
#             "Appointment of Director",
#             "Appointment of Chairman",
#             "Appointment of Chairman and Managing Director",
#             "Appointment of Chief Executive Officer (CEO)",
#             "Appointment of Chief Financial Officer (CFO)",
#             "Appointment of Managing Director & CEO",
#             "Increase of Authorised Capital",
#             "Board Meeting Deferred",
#             "Board Meeting Postponed",
#             "Board Meeting Rescheduled",
#             "Board Meeting Cancelled",
#             "Resignation of Director",
#             "Resignation of Chairman",
#             "Resignation of Chairman and Managing Director",
#             "Resignation of Chief Executive Officer (CEO)",
#             "Resignation of Chief Financial Officer (CFO)",
#             "Resignation of Managing Director",
#             "Investor Presentation",
#             "Change in Management",
#             "Issue of Securities",
#             "Offer for Sale",
#             "Open Offer - Updates",
#             "Post Buyback Public Announcement",
#             "Post Offer Public Announcement",
#             "Preferential Issue",
#             "Qualified Institutional Placement",
#             "Raising of Funds",
#             "Restructuring",
#             "Earnings Call Transcript",
#             "Public Announcement",
#             "Revision of outcome",
#             "Funds raising by issuance of Debt Securities by Large Entities",
#             "Amalgamation/ Merger",
#             "De-merger",
#             "Acquisition",
#             "Update-Acquisition/Scheme/Sale/Disposal/Reg30",
#             "Change in Directors/ Key Managerial Personnel/ Auditor/ Compliance Officer/ Share Transfer Agent",
#         ]

#         # "Company Update": ["General", "Award of Order/Receipt of Order"]
#     }

#     base_url = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"

#     # Get today's date and 1 year prior in the format YYYYMMDD
#     today = datetime.now().strftime("%Y%m%d")
#     if strCat == "Result":
#         d = 730
#     else:
#         d = 364
#     from_ = (datetime.now() - timedelta(days=d)).strftime("%Y%m%d")

#     # Use provided date or default to calculated dates
#     strToDate = strToDate if strToDate else today
#     strPrevDate = strPrevDate if strPrevDate else from_
    
#     params = {
#         "pageno": pageno,
#         "strCat": strCat,
#         "strPrevDate": strPrevDate,
#         "strScrip": strScrip,
#         "strSearch": strSearch,
#         "strToDate": strToDate,
#         "strType": strType,
#         "subcategory": subcategory
#     }
    
#     headers = {
#         "referer": "https://www.bseindia.com/",
#         "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
#     }
    
#     response = requests.get(base_url, params=params, headers=headers)
    
#     if response.status_code == 200:
#         data = response.json()
#         filtered_data = []

#         # Get valid subcategories for the current category
#         valid_subcategories = category_subcategory_map.get(strCat, [])
        
#         if valid_subcategories:
#             # Filter data based on subcategories
#             for item in data["Table"]:
#                 subcategory_name = item.get("SUBCATNAME", "").strip()
#                 if subcategory_name in valid_subcategories:
#                     filtered_data.append(item)
#             # return filtered_data  # Keep the existing limit of 2 items
#             return filtered_data[:2]  # Keep the existing limit of 2 items
#         else:
#             # If no subcategories defined, return original data with limit
#             # return data["Table"]
#             return data["Table"][:2]

#         # data = data["Table"][:2]
#         # return data
#     else:
        
#         # return {
#         #     "Error": response.status_code,
#         #     "Message": response.text
#         # }
#         return []



def fetch_bse_data(
    pageno: str = "1",
    strCat: str = "Company Update",
    strPrevDate: Optional[str] = None,
    strScrip: str = "538734",
    strSearch: str = "P",
    strToDate: Optional[str] = None,
    strType: str = "C",
    subcategory: str = "-1"
) -> list:
    """
    Fetch data from the BSE API using the specified parameters.
    Defaults are used if values are not provided by the user.
    
    Parameters:
    - pageno: The page number for pagination (default: "1").
    - strCat: Category of the data, e.g., 'Board Meeting', 'AGM/EGM', 'Result' (default: 'Board Meeting').
    - strPrevDate: Starting date of the range in YYYYMMDD format (default: "20240718").
    - strScrip: Company Scrip Code (default: "532215").
    - strSearch: Search type (default: "P").
    - strToDate: End date of the range in YYYYMMDD format (default: "20241018").
    - strType: Type of the announcement (default: "C").
    - subcategory: Subcategory ID (default: "-1").
    - base_url: The base URL for the API request (default: BSE announcement API URL).

    Returns:
    - list of information if the request is successful.
    - Error message if the request fails.
    """
    category_subcategory_map = {
        "AGM/EGM": ["AGM"],
        "Board Meeting": ["Board Meeting", "Outcome of Board Meeting"],
        "Corp. Action": ["Sub-division/Stock Split", "Record Date", "Dividend", "Bonds/Right Issue", "Bonus"],
        "Result": ["Financial Results"],
        "Company Update": [
            "General",
            "Acquisition",
            "Allotment of Equity Shares",
            "Allotment of Warrants",
            "Award of Order / Receipt of Order",
            "Buy back",
            "Delisting",
            "Joint Venture",
            "Open Offer",
            "Debt Securities",
            "Credit Rating",
            "Shareholding",
            "Appointment of Director",
            "Appointment of Chairman",
            "Appointment of Chairman and Managing Director",
            "Appointment of Chief Executive Officer (CEO)",
            "Appointment of Chief Financial Officer (CFO)",
            "Appointment of Managing Director & CEO",
            "Increase of Authorised Capital",
            "Board Meeting Deferred",
            "Board Meeting Postponed",
            "Board Meeting Rescheduled",
            "Board Meeting Cancelled",
            "Resignation of Director",
            "Resignation of Chairman",
            "Resignation of Chairman and Managing Director",
            "Resignation of Chief Executive Officer (CEO)",
            "Resignation of Chief Financial Officer (CFO)",
            "Resignation of Managing Director",
            "Investor Presentation",
            "Change in Management",
            "Issue of Securities",
            "Offer for Sale",
            "Open Offer - Updates",
            "Post Buyback Public Announcement",
            "Post Offer Public Announcement",
            "Preferential Issue",
            "Qualified Institutional Placement",
            "Raising of Funds",
            "Restructuring",
            "Earnings Call Transcript",
            "Public Announcement",
            "Revision of outcome",
            "Funds raising by issuance of Debt Securities by Large Entities",
            "Amalgamation/ Merger",
            "De-merger",
            "Acquisition",
            "Update-Acquisition/Scheme/Sale/Disposal/Reg30",
            "Change in Directors/ Key Managerial Personnel/ Auditor/ Compliance Officer/ Share Transfer Agent",
        ]

        # "Company Update": ["General", "Award of Order/Receipt of Order"]
    }

    base_url = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"

    # Get today's date and 1 year prior in the format YYYYMMDD
    today = datetime.now().strftime("%Y%m%d")
    if strCat == "Result":
        d = 730
    else:
        d = 364
    from_ = (datetime.now() - timedelta(days=d)).strftime("%Y%m%d")

    # Use provided date or default to calculated dates
    strToDate = strToDate if strToDate else today
    strPrevDate = strPrevDate if strPrevDate else from_
    
    params = {
        "pageno": pageno,
        "strCat": strCat,
        "strPrevDate": strPrevDate,
        "strScrip": strScrip,
        "strSearch": strSearch,
        "strToDate": strToDate,
        "strType": strType,
        "subcategory": subcategory
    }
    
    headers = {
        "referer": "https://www.bseindia.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
    }
    
    response = requests.get(base_url, params=params, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        valid_subcategories = category_subcategory_map.get(strCat, [])
        
        if valid_subcategories:
            # Group items by subcategory
            subcategory_groups = defaultdict(list)
            for item in data["Table"]:
                subcategory_name = item.get("SUBCATNAME", "").strip()
                if subcategory_name in valid_subcategories:
                    subcategory_groups[subcategory_name].append(item)
            
            # Take top 2 items from each subcategory
            filtered_data = []
            for subcategory_items in subcategory_groups.values():
                filtered_data.extend(subcategory_items[:2])
            
            return filtered_data
        
        return data["Table"][:2]
    
    return []

def process_fetched_data(data : list) -> list:
    """
    Process the fetched data to extract relevant information.
    
    Parameters:
    - data: List of announcements fetched from the BSE API.
    
    Returns:
    - List of dictionaries containing the relevant information.
    """

    if data == None:
        return []
    
    processed_data = []
    
    for item in data:

        if(item != None):
            company_code = item["SCRIP_CD"]
            pdf_name = item["ATTACHMENTNAME"]
            category_name = item["CATEGORYNAME"]
            subcategory_name = item.get("SUBCATNAME", "").strip()
            upload_dt = item["DT_TM"]
            try:
                # First try with microseconds
                dt_object = datetime.strptime(upload_dt, '%Y-%m-%dT%H:%M:%S.%f')
            except ValueError:
                try:
                    # If that fails, try without microseconds
                    dt_object = datetime.strptime(upload_dt, '%Y-%m-%dT%H:%M:%S')
                except ValueError:
                    # If both fail, log the error and skip this item
                    print(f"Error processing date format for {upload_dt}")
                    continue
            # dt_object = datetime.strptime(upload_dt, '%Y-%m-%dT%H:%M:%S.%f')
            upload_date = dt_object.date()

            processed_data.append({
                "company_code": company_code,
                "pdf_name": pdf_name,
                "category_name": category_name,
                "subcategory_name": subcategory_name,
                "upload_date": str(upload_date)
            })
        else:
            print("Error processing data")
        # except Exception as e:
        #     print(f"Error processing data: {e}")
    
    return processed_data

def download_pdf_from_link(pdf_link: str) -> str:
    """
    Download and extract text from a PDF given its URL.
    
    Parameters:
    - pdf_link: Full URL to the PDF file
    
    Returns:
    - Extracted text content from the PDF
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

    session = requests.Session()
    response = session.get(pdf_link, headers=headers, allow_redirects=True)
    
    try:
        pdf_content = io.BytesIO(response.content)
        raw_text = extract_text(pdf_content)
        return raw_text
    except Exception as e:
        print(f"Error downloading PDF: {e}")
        return ""

def download_pdf(pdf_name: str) -> str:
    """
    Download a PDF from the BSE website and extract its text content.

    Parameters:
    - pdf_name: The name of the PDF file to be downloaded.

    Returns:
    - The extracted text content from the PDF.
    """
    pdf_url = f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{pdf_name}"

    return download_pdf_from_link(pdf_url)

def process_pdf_and_insert(doc_id, company_id, pdf_name, pdf_text, supabase_client):
# def process_pdf_and_insert(document_id, pdf_name, pdf_text, supabase_client, vector_store):

    # Split text into smaller chunks using RecursiveCharacterTextSplitter
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=20000, chunk_overlap=200)
    chunks = text_splitter.split_text(pdf_text)

    docs = []

    for idx, chunk in enumerate(chunks):
        # Generate a hash for the chunk to avoid duplicates
        content_hash = hashlib.sha256(chunk.encode('utf-8')).hexdigest()

        # Check if chunk already exists in the database (avoid duplicates)
        existing_chunk = supabase_client.table('chunks_v2').select('id').eq('metadata->>content_hash', content_hash).execute()
        # existing_chunk = supabase_client.table('chunks_v2').select('id').eq('metadata->>document_id', str(doc_id)).eq('metadata->>content_hash', content_hash).execute()
        if existing_chunk.data:
            print(f"Chunk {idx} already exists, skipping.")
            continue
        
        # Store the chunk as a text file in Supabase storage (you could also store the content directly in the DB)
        blob_file_path = f"test/{pdf_name}_chunk_{idx}.txt"
        
        try:
            supabase_client.storage.from_("pdf_chunks").update(blob_file_path, chunk.encode('utf-8'), {
                'contentType': 'text/plain'
            })
        except:
            supabase_client.storage.from_("pdf_chunks").upload(blob_file_path, chunk.encode('utf-8'), {
                'contentType': 'text/plain'
            })

        document = Document(
            page_content=blob_file_path,
            metadata={
                'document_id': str(doc_id),
                'content_hash': content_hash,
                'company_id': str(company_id)
                # 'content_link': blob_file_path,
            }
        )

        docs.append(document)

        # print(pdf_name, idx, content_hash, blob_file_path)
        
    
    embeddings = OpenAIEmbeddings(model="text-embedding-3-small")

    SupabaseVectorStore.from_documents(
        docs,
        embeddings,
        client = supabase_client,
        table_name = "chunks_v2",
        query_name="match_chunks_v2",
        chunk_size=20000,
    )

    print("PDF processing complete!")

def upload_documents_info(data: list, supabase_client):
    """
    Upload the processed data to the Supabase database.

    Parameters:
    - data: List of dictionaries containing the processed data.
    - supabase_client: Supabase client instance.
    """
    for item in data:
        company_code = item["company_code"]
        pdf_name = item["pdf_name"]
        category_name = item["category_name"]
        upload_date = item["upload_date"]
        subcategory_name = item["subcategory_name"]
        company_id = get_company_name(company_code)
        # print(company_code, pdf_name, category_name, upload_date)
        
        # print(f"Processing document: {pdf_name}")
        
        # Check if the document already exists in the database
        existing_document = supabase_client.table('documents').select('id').eq('pdf_name', pdf_name).execute()
        if existing_document.data:
            print(f"Document {pdf_name} already exists, skipping.")
            continue

        doc_id = str(uuid.uuid4())

        # Insert document data into the documents table
        supabase_client.table('documents').insert({
            'id': doc_id,
            'company_code': company_code,
            'pdf_name': pdf_name,
            'category_name': category_name,
            'subcategory_name': subcategory_name,
            'upload_date': upload_date,
            'company_id': company_id
        }).execute()

        if category_name == "Result":
            if fin_extractor.load_pdf(pdf_name, is_url=True):
                # Process the PDF
                print(f"Processing financial data for {pdf_name}")
                content, tables = fin_extractor.process_pdf()
                print("Content and table present")
                fin_extractor.upload_results(get_company_name(str(company_code)), doc_id, pdf_name, supabase_client, content, tables)
        else:
            # Add it to chunks table
            pdf_content = download_pdf(pdf_name)
            process_pdf_and_insert(doc_id, company_id, pdf_name, pdf_content, supabase_client)

        
    
    # print("Document info upload complete!")

def get_all_data(scrip_code):
    print("Working on scrip code:", scrip_code)
    category_lsit = ["Board Meeting", "AGM/EGM", "Company Update", "Result"]
    for category in category_lsit:
        print("Working on category:", category)
        fetched_data = fetch_bse_data(strCat=category, strScrip=scrip_code)
        
        process_data = process_fetched_data(fetched_data)
        supabase_client = create_supabase_client()
        upload_documents_info(process_data, supabase_client)
        # print("upload data")

def get_all_data_from_name(name:str):
    print("Working on scrip name:", name)
    scrip_code = get_security_code(name)
    print("Working on scrip code:", scrip_code)
    get_all_data(scrip_code)

def generate_summary(text: str, custom_prompt: Optional[str] = None) -> str|None:
    """
    Generate a summary of the given text using Perplexity API.
    
    Parameters:
    - text: Text content to summarize
    - custom_prompt: Optional custom system prompt for specific summary requirements
    
    Returns:
    - Generated summary text
    """
    try:

        client = openai.OpenAI(
            api_key=os.getenv("PERPLEXITY_API_KEY"), base_url="https://api.perplexity.ai"
        )
        
        default_prompt = "You are a helpful assistant that summarizes corporate announcements. Provide concise, factual summaries focusing on key information and business impact."
        system_prompt = custom_prompt or default_prompt
        
        # Combine system prompt and user content
        full_prompt = f"{system_prompt}\n\nPlease provide a brief summary of this corporate announcement in 2-3 sentences: {text[:4000]}"
        
        response = client.chat.completions.create(
            model="llama-3.1-sonar-small-128k-online",
            messages=[{
                "role": "user",
                "content": full_prompt
            }]
        )
        
        return response.choices[0].message.content
    except Exception as e:
        print(f"Error generating summary with Perplexity: {e}")
        return ""  # Explicitly return empty string instead of None

# get_all_data_from_name("WIPRO") # 502175
# fetch_bse_data(strCat="Company Update", strScrip="502175")

# pdf_name = "257a1ab5-d329-4db6-9c5a-803e9880dac6.pdf"
# raw_text = download_pdf(pdf_name)
# process_pdf_and_insert("test_doc_id", pdf_name, raw_text, supabase_client)





