from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import SupabaseVectorStore
from utils.supabase_client import supabase_client as supabase
from .retrival_utils import get_security_code, get_document_ids

def get_relevant_docs(company_name, query):
    embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
    security_code = get_security_code(company_name)
    if not security_code:
        print(f"No security code found for company: {company_name}")
        return []

    document_ids = get_document_ids(security_code)
    if not document_ids:
        print(f"No documents found for security code: {security_code}")
        return []
    
    vector_store = SupabaseVectorStore(
        embedding=embeddings,
        client=supabase,
        table_name="chunks_v2",
        query_name="match_chunks_v2",
        # filter={"metadata->>document_id": str(document_ids[0])}
    )
    matched_docs_list = []
    for doc_id in document_ids:
        matched_docs = vector_store.similarity_search(
            query=query,
            k = 5,
            filter={
                "document_id": doc_id
            },
        )
        matched_docs_list.extend(matched_docs)

    return matched_docs_list
    

def download_and_extract(doc_list):
    content = ""
    try:
        for doc in doc_list:
            file_path = doc.page_content
            response = supabase.storage.from_("pdf_chunks").download(file_path)
            # print(response.decode('utf-8'))
            content += response.decode('utf-8') + "\n\n\n\n\n"
            return content
    except Exception as e:
        print(f"Error downloading file from blob: {str(e)}")
        return None

