from fuzzywuzzy import fuzz, process

# def validate_company_name(input_name: str, company_list:list) -> list:
#     """
#     Recommend the closest company name(s) based on the input.

#     Args:
#         input_name (str): The input company name to validate.
#         company_list (list): A list of company names to compare with.

#     Returns:
#         list: A list of recommended company names based on the input.
#     """
#     # Normalize the input
#     input_name = input_name.strip().lower()
    
#     # First, try to find exact matches
#     exact_matches = [name for name in company_list if name.lower() == input_name]
    
#     if exact_matches:
#         return exact_matches
    
#     # If no exact matches, find all names containing the input (partial matches)
#     partial_matches = [name for name in company_list if input_name in name.lower()]
    
#     if partial_matches:
#         return partial_matches
    
#     # If no matches found, use fuzzy matching for suggestions
#     best_match, score = process.extractOne(input_name, company_list, scorer=fuzz.ratio)
    
#     print(f"Input: {input_name}, Best Match: {best_match}, Score: {score}")  # Debugging output
    
#     # Provide feedback based on the best match score
#     if score >= 80:  # Adjust this threshold as needed
#         return [best_match]
#     else:
#         # Suggest multiple close matches if score is lower
#         close_matches = process.extract(input_name, company_list, scorer=fuzz.ratio, limit=5)
#         suggestions = [match[0] for match in close_matches if match[1] >= 45]  # Threshold can be adjusted
#         if suggestions:
#             return suggestions
#         else:
#             return ["No suggestions found. Please check the spelling."]


def validate_company_name(input_name: str, company_list: list) -> dict:
    """
    Recommend the closest company name(s) based on the input.

    Args:
        input_name (str): The input company name to validate.
        company_list (list): A list of company names to compare with.

    Returns:
        dict: A dictionary containing the message and recommended company names.
    """
    # Normalize the input
    input_name = input_name.strip().lower()
    
    # First, try to find exact matches
    exact_matches = [name for name in company_list if name.lower() == input_name]
    
    if exact_matches:
        return exact_matches
        # return {"message": "Exact match found.", "matches": exact_matches}
    
    # If no exact matches, find all names containing the input (partial matches)
    partial_matches = [name for name in company_list if input_name in name.lower()]
    
    if partial_matches:

        if len(partial_matches) == 1:
            return partial_matches
        
        return {
            "message": f"No exact match found for '{input_name}'. Did you mean one of these? Please enter the specific company name.",
            "matches": partial_matches
        }
    
    # If no matches found, use fuzzy matching for suggestions
    best_match, score = process.extractOne(input_name, company_list, scorer=fuzz.ratio)
    
    if score >= 80:  # If the fuzzy match score is high, return the best match
        return {
            "message": f"It looks like you might have meant '{best_match}'.",
            "matches": [best_match]
        }
    else:
        # Suggest multiple close matches if the score is lower
        close_matches = process.extract(input_name, company_list, scorer=fuzz.ratio, limit=5)
        suggestions = [match[0] for match in close_matches if match[1] >= 45]  # Threshold can be adjusted
        if suggestions:
            return {
                "message": f"It seems you might have mis-typed '{input_name}'. Which of the following did you mean?",
                "matches": suggestions
            }
        else:
            return {
                "message": "No suggestions found. Please check the spelling or try a different name.",
                "matches": []
            }

        