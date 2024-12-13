from supabase import create_client
from langchain_community.embeddings import OpenAIEmbeddings
import os
from dotenv import load_dotenv

load_dotenv()

# Initialize your Supabase client here
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase_client = create_client(url, key)


# def get_client():
#     return supabase_client

def create_supabase_client():
    return create_client(url, key)
