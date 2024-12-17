import pdfplumber
import io
import os
import requests
from typing import List, Any, Tuple, Dict
import re
from typing import Optional
import fitz  # PyMuPDF
from PIL import Image
import google.generativeai as genai
import base64
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.database.retrival_utils import get_company_full_name
from dotenv import load_dotenv
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import SupabaseVectorStore
from langchain.schema import Document
from langchain_openai import OpenAIEmbeddings
import hashlib
import openai

load_dotenv()

class FinancialContentExtractor:
    def __init__(self, google_api_key: str|None):
        """
        Initialize the PDF Content Extractor
        
        Args:
            google_api_key: API key for Google Generative AI
        """
        self.current_pdf = None
        self.fitz_doc = None
        # Configure Google Generative AI
        genai.configure(api_key=google_api_key)
        # Get the model
        generation_config = {
        "temperature": 1,
        "top_p": 0.95,
        "top_k": 40,
        "max_output_tokens": 8192,
        "response_mime_type": "text/plain",
        }
        self.model = genai.GenerativeModel('gemini-1.5-flash-002', generation_config= generation_config)
        # Initialize content storage
        self.content = []  # Will store text in order
        self.tables = []   # Will store extracted tables
        
    def load_pdf(self, source: str, is_url: bool = False) -> bool:
        """Load PDF file and initialize both pdfplumber and fitz"""
        try:
            if is_url:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'referer': 'https://www.bseindia.com/'
                }
                response = requests.get(f"https://www.bseindia.com/xml-data/corpfiling/AttachHis/{source}", headers=headers)
                pdf_data = io.BytesIO(response.content)
            else:
                with open(source, 'rb') as file:
                    pdf_data = io.BytesIO(file.read())
            
            # Initialize both PDF readers with the BytesIO object
            self.current_pdf = pdfplumber.open(pdf_data)
            self.fitz_doc = fitz.open(stream=pdf_data.getvalue(), filetype="pdf")
            return True
        except Exception as e:
            print(f"Error loading PDF: {str(e)}")
            return False

    def has_table(self, page) -> bool:
        """Determine if a page contains a table"""
        try:
            page_text = page.extract_text() or ""
            
            # Check for financial keywords
            keywords = [
                r'\b(?:year|quarter|month)(?:\s+ended|\s+ending)\b',
                r'\bbalance sheet\b', r'\bcash flow\b',
                r'\bfinancial results\b', r'\bcrore\b', r'\bRs\.',
                r'\bparticulars\b', r'\btotal\b'
            ]
            has_keywords = any(re.search(pattern, page_text, re.IGNORECASE) 
                             for pattern in keywords)
            
            # Check for numeric patterns
            numeric_lines = len(re.findall(r'\d{1,3}(?:,\d{3})*(?:\.\d+)?', page_text))
            total_lines = len(page_text.split('\n'))
            numeric_ratio = numeric_lines / total_lines if total_lines > 0 else 0
            
            # Check table structure
            has_structure = (
                len(page.horizontal_edges) > 2 and 
                len(page.vertical_edges) > 2
            )
            
            return (has_keywords and numeric_ratio > 0.2) or has_structure
            
        except Exception as e:
            print(f"Error checking for table: {str(e)}")
            return False

    def page_to_image_bytes(self, page_num: int) -> bytes:
        """Convert a single page to image bytes using PyMuPDF"""
        try:
            # Get the page
            page = self.fitz_doc[page_num - 1]
            
            # Set resolution
            zoom = 2  # zoom factor
            mat = fitz.Matrix(zoom, zoom)
            
            # Get the pixmap
            pix = page.get_pixmap(matrix=mat)
            
            # Convert to bytes
            img_bytes = pix.tobytes("png")
            
            return img_bytes
            
        except Exception as e:
            print(f"Error converting page to image: {str(e)}")
            return None

    def extract_table_with_gemini(self, image_bytes: bytes) -> str:
        """Extract table data using Gemini Vision"""
        try:
            # Convert bytes to PIL Image
            image = Image.open(io.BytesIO(image_bytes))
            
            prompt = """
            Please extract all tabular data from this financial report image with high accuracy. The image may contain financial tables showing quarterly, half-yearly, and annual financial performance, structured with rows and columns representing various financial metrics.

            Key details:

            Capture all rows, columns, and headers as they appear in the tables.
            Ensure data fields include specific categories such as 'Income', 'Expenses', 'Profit Before Tax', 'Segment Revenue', and 'Segment Results' along with corresponding figures for each reporting period (e.g., quarterly, half-yearly, and annually).
            Accurately capture and align any sub-totals, totals, or sub-headings (e.g., 'Income,' 'Expenses,' 'Segment Revenue').
            Retain all numerical formatting, including positive and negative signs, currency symbols, and units (e.g., ₹ in crores or lakhs).
            Include any footnotes, asterisks, or notes associated with the table, maintaining their connection to relevant data points in the table.
            The tables may vary in format, but each should be extracted with all structural details preserved, allowing for accurate and clean data extraction from different financial report layout
            """
            
            response = self.model.generate_content([prompt, image])
            return response.text
            
        except Exception as e:
            print(f"Error extracting table with Gemini: {str(e)}")
            return ""

    def process_pdf(self) -> Tuple[List[str], List[Dict]]:
        """
        Process entire PDF, extracting both text and tables
        
        Returns:
            Tuple containing:
            - List of text content
            - List of extracted tables with metadata
        """
        if not self.current_pdf or not self.fitz_doc:
            print("No PDF loaded. Call load_pdf() first.")
            return [], []
            
        self.content = []
        self.tables = []
        
        print("length of pages in pdf", len(self.current_pdf.pages))
        
        for page_num, page in enumerate(self.current_pdf.pages[:50], 1):
            try:
                
                if self.has_table(page) or len(self.current_pdf.pages) < 15:
                    print(page_num)
                    # Convert page to image bytes and extract table
                    image_bytes = self.page_to_image_bytes(page_num)
                    if image_bytes:
                        table_data = self.extract_table_with_gemini(image_bytes)
                        if table_data:
                            self.tables.append({
                                'page_number': page_num,
                                'table_data': table_data,
                                'image_bytes': base64.b64encode(image_bytes).decode('utf-8')  # Store encoded image bytes
                            })
                            # self.content.append(f"[Table on page {page_num}]")
                # else:
                # Extract text from non-table page using PyMuPDF
                fitz_page = self.fitz_doc[page_num - 1]
                text = fitz_page.get_text()
                if text:
                    self.content.append(text)
                        
            except Exception as e:
                print(f"Error processing page {page_num}: {str(e)}")
                self.content.append(f"[Error processing page {page_num}]")
                
        return self.content, self.tables

    def get_results(self) -> Dict:
        """
        Get the extracted content in a structured format
        
        Returns:
            Dict containing:
            - full_text: Complete text content
            - tables: List of extracted tables
        """
        return {
            'full_text': '\n'.join(self.content),
            'tables': [{
                'page_number': table['page_number'],
                'table_data': table['table_data'],
                'image_bytes': table['image_bytes']  # Base64 encoded image bytes
            } for table in self.tables]
        }

    def generate_summary(self, name:str, text: str, prompt:str = None) -> str|None:
        """
        Generate a summary of the given text using Perplexity API.
        
        Parameters:
        - text: Text content to summarize
        - custom_prompt: Optional custom system prompt for specific summary requirements
        
        Returns:
        - Generated summary text
        """
        full_name = get_company_full_name(name)
        try:

            client = openai.OpenAI(
                api_key=os.getenv("PERPLEXITY_API_KEY"), base_url="https://api.perplexity.ai"
            )
            
            # Combine system prompt and user content
            full_prompt = f"""
            You are an expert in INDIAN EQUITY MARKET. And have been asked to analyse the financial reports of {full_name}({name}).
            Here is the text showing this quarters financial results for {full_name}. STRICTLY USE THE CONTEXT PROVIDED BELOW TO GENERATE THE RESPONSE. 
            DO NOT USE ANY OTHER INFORMATION.
            Please generate a summary for the this quaters results, show me the QoQ and YoY change 
            in the most important financial metrics. By QoQ i mean quarter on quater, only compare 
            last quarter results to this. For example, you will compare September 2024 results to June 
            2024 (therefore a difference of 4 months) By YoY i mean year on year, only compare last year 
            results to this (not last 6 months)For examle, september 2024 results will be compared to september 2023.
            Use bullet points for each metric. Make sure your response is just one paragraph and is well structured. 
            Use crores and rupees only, percentages for changes:

            {text}
            """

            if prompt is not None:
                full_prompt += f"\n{prompt}\n\n{text}"
            
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

    def _clean_tables_data(self, text):
        """
        Extract only sections containing markdown tables and skip sections with "no table" messages.
        """
        # Split text by the separator
        sections = text.split("---------------------------------------------------------------------------------------------------")
        
        # Skip phrases that indicate no tables
        skip_phrases = [
            "no tabular data",
            "cannot extract tabular",
            "does not contain any tabular",
            "no tables",
        ]
        
        cleaned_sections = []
        
        for section in sections:
            # Skip if section contains any of the skip phrases
            if any(phrase in section.lower() for phrase in skip_phrases):
                continue
                
            # Skip if section doesn't contain any markdown tables (checking for |)
            if '|' not in section:
                continue
            
            # Add non-empty cleaned section
            cleaned_section = section.strip()
            if cleaned_section:
                cleaned_sections.append(cleaned_section)
        
        return '\n\n' + '='*80 + '\n\n'.join(cleaned_sections)

    def upload_results(self, company_name: str, doc_id: str, pdf_name: str, supabase_client, content: list, category_name, subcategory_name, tables: list[dict[Any, Any]]) -> bool:
        """
        Upload the extracted content to a cloud storage bucket
        
        Args:
            company_name: Name of the company
            doc_id: Unique document ID
            pdf_name: Name of the PDF file
            supabase_client: Supabase client object
            content: List of text content
            tables: List of extracted tables
        
        Returns:
            True if upload was successful, False otherwise
        """
        try:
            # upload content to chunks_v2
            # Split text into smaller chunks using RecursiveCharacterTextSplitter
            if len(content) != 0 or content is not None:
                print("Uploading content to Supabase...")
                join_content = '\n'.join(content)
                text_splitter = RecursiveCharacterTextSplitter(chunk_size=20000, chunk_overlap=200)
                chunks = text_splitter.split_text(join_content)

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
                            'company_id': company_name,
                            'category_name': category_name,
                            'subcategory_name': subcategory_name,
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

                print("done uploading content to Supabase...")

            if len(tables) != 0 or tables is not None:
                print("Uploading tables to Supabase...")
                table_data = ''
                for tab in tables:
                    table_data += tab['table_data'] + "\n\n ---------------------------------------------------------------------------------------------------\n\n"
                # Store the chunk as a text file in Supabase storage (you could also store the content directly in the DB)
                blob_file_path = f"test_tables/{pdf_name}.txt"

                table_data = self._clean_tables_data(table_data)

                try:
                    # Try to update first
                    supabase_client.storage.from_("pdf_chunks").update(
                        blob_file_path,
                        table_data.encode('utf-8'),
                        {'contentType': 'text/plain'}
                    )
                except:
                    # If update fails (file doesn't exist), upload it
                    supabase_client.storage.from_("pdf_chunks").upload(
                        blob_file_path,
                        table_data.encode('utf-8'),
                        {'contentType': 'text/plain'}
                    )

                # For the database table, you can still use upsert
                supabase_client.table('financial_report_tables').upsert({
                    'document_id': doc_id,
                    'company_id': company_name,
                    'content': blob_file_path,
                    'summary': self.generate_summary(company_name, table_data)
                }).execute()
                
            return True
        except Exception as e:
            print(f"Error uploading results: {str(e)}")

    def close(self):
        """Clean up resources"""
        if self.current_pdf:
            self.current_pdf.close()
        if self.fitz_doc:
            self.fitz_doc.close()


# # # Example usage
# api_key = os.getenv("GEMINI_API_KEY")
# # pdf_url = "https://www.bseindia.com/xml-data/corpfiling/AttachHis/a79f8511-fbee-457b-99a8-aaf468780c1a.pdf"
# pdf_url = "https://www.bseindia.com/xml-data/corpfiling/AttachHis/6dcf40b2-c9e2-47a6-8771-4338212fb286.pdf"
# pdf_name = "3bc577e6-caa7-44b8-a0da-efd6785ce4aa.pdf"

# extractor = FinancialContentExtractor(api_key)

# try:
#     if extractor.load_pdf(pdf_name, is_url=True):
#         # Process the PDF
#         content, tables = extractor.process_pdf()
        
#         # Print full text content
#         print("\nDocument Content:")
#         print("=" * 50)
#         print('\n'.join(content))
        
#         # Print extracted tables
#         print("\nExtracted Tables:")
#         print("=" * 50)
#         for table in tables:
#             print(f"\nTable from page {table['page_number']}:")
#             print("-" * 40)
#             print(table['table_data'])
#             # Image bytes are stored in table['image_bytes'] as base64
        
#         table_data = ''
#         for tab in tables:
#             table_data += tab['table_data'] + "\n\n ---------------------------------------------------------------------------------------------------\n\n"
                
#         summary = extractor.generate_summary(table_data)
#         print("\nGenerated Summary:\n", summary)
            
# finally:
#     extractor.close()
