from utils import data_loader, supabase_client
from data_collection.bse_data_collection import fetch_bse_data, process_fetched_data, upload_documents_info, get_all_data
from utils.database.retrival_utils import get_all_securities_codes
from concurrent.futures import ThreadPoolExecutor
import time


def process_batch(batch):
    return [get_all_data(code) for code in batch]

def divide_into_sublists(lst, num_sublists):
    # Calculate the size of each sublist
    n = len(lst) // num_sublists  # Number of elements per sublist
    extra = len(lst) % num_sublists  # Remaining elements to distribute

    # Create sublists
    sublists = [lst[i * n + min(i, extra):(i + 1) * n + min(i + 1, extra)] for i in range(num_sublists)]
    return sublists




if __name__ == "__main__":


    # Initialize company names when the server starts
    # data_loader.initialize_company_names()
    # company_list = data_loader.company_names
    # print(company_list)

    # vector_store = 

    # Fetch data from the BSE API
    fetched_data = get_all_data("500002") # 532215    500209
    process_data = process_fetched_data(fetched_data)
    # upload_documents_info(process_data, supabase_client.get_client())
    print(process_data)

    # batches = get_all_securities_codes()
    # batches = divide_into_sublists(batches, 4)
    # batches = [["506597"]]
    # batches = ["526371", "509930", "500260", "500144"]
    # # # all present
    # batches = [["513377", "539150"], ["505412", "500108"], ["500171", "502219", "532440"], ["502090", "512229", "526227", "534425 "]]

    # batches = [["539788", "539132"], ["526654", "532334"], ["513536", "540175", "535566"], ["500282", "533343", "530185", "526971 "]]

    # for i in range(0, len(batches)):
    #     batch = batches[i]
    #     print(f"Processing batch: {batch} which is on index: {i}")
    #     get_all_data(batch)


    # start_time = time.time()
    # with ThreadPoolExecutor(max_workers=4) as executor:
    #     results = list(executor.map(process_batch, batches))
    # print(f"Time taken: {time.time() - start_time}")

    # print(batches)
