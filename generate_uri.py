import hashlib
import json
import argparse
import requests
from tqdm import tqdm
from requests.exceptions import RequestException
from tenacity import retry, stop_after_attempt, wait_fixed
import os

# Retry settings: 5 attempts with a 2-second wait between each
@retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
def query_viaf_cluster(keyword):
    """
    Query the VIAF AutoSuggest API for matching terms and return multiple results.

    Args:
    keyword (str): The keyword to search for.
    """
    url = "https://www.viaf.org/viaf/AutoSuggest"
    params = {
        "query": keyword,
        "index": 'lc',
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()  # Raise an exception for HTTP errors

    return response.json()

def generate_uri(input_string):
    """
    Generate a URI from a string by hashing it and taking the first 8 characters.

    Args:
    input_string (str): The string to hash.
    """
    s = input_string.lower().replace(' ', '').replace(',', '')

    # Convert the string to bytes
    byte_string = s.encode('utf-8')

    # Hash the byte string
    hash_object = hashlib.sha256(byte_string)

    # Get the hexadecimal digest
    hex_digest = hash_object.hexdigest()

    # Convert the first 8 characters to an integer
    hash_int = int(hex_digest[:8], 16) % 100000000
    
    return 'r' + str(hash_int)

def process_viaf_response(response, document_date=None):
    """
    Process the VIAF response and return the most relevant record based on date filtering.

    Args:
    response (dict): The response from the VIAF API.
    document_date (str): The document date for filtering the results.
    """
    if response and 'result' in response and response['result']:
        candidates = []
        for record in response['result']:
            # We want to match on a specific date if possible
            viaf_id = record.get('viafid')
            name = record.get('term')
            lc_id = record.get('lcid') or record.get('lc')
            
            # Get year from the record if possible
            record_year = record.get('dates')

            # Filter based on the document date
            if document_date and record_year:
                if str(document_date) not in record_year:
                    continue

            candidates.append({
                'viaf': viaf_id,
                'lc': lc_id,
                'name': name,
                'year': record_year
            })

        # Sort by relevance if we have any candidates
        if candidates:
            return sorted(candidates, key=lambda x: x['year'] if x['year'] is not None else float('inf'))[0] if candidates else None

    return None

def add_viaf_uris(term_obj, term_type, document_date, debug_responses):
    """
    Add VIAF URIs to the JSON data, filtering by document date.

    Args:
    term_obj (dict): The term object.
    term_type (str): The type of term (e.g., 'person', 'place', etc.).
    document_date (str): The date from the parent document.
    debug_responses (dict): Dictionary to store debug responses.
    """
    # Check if the 'term' key exists in the term_obj
    term_value = term_obj.get('term') or term_obj.get('name')
    if not term_value:
        print(f"Warning: No 'term' or 'name' key found in term_obj: {term_obj}")
        return term_obj

    try:
        viaf_response = query_viaf_cluster(term_value)
        debug_responses[term_value] = viaf_response  # Save the response for debugging
        
        if viaf_response and 'result' in viaf_response:
            viaf_record = process_viaf_response(viaf_response, document_date)
            if viaf_record and viaf_record.get('viaf'):
                term_obj['viaf'] = viaf_record['viaf']
                term_obj['lc'] = viaf_record.get('lc')
    except RequestException as e:
        print(f"Error querying VIAF API for term '{term_value}': {e}")
    
    return term_obj

def add_uris_to_json(json_data):
    """
    Add URIs to the JSON data.
    
    Args:
    json_data (dict): The JSON data.
    """
    debug_responses = {}  # Dictionary to store API responses for debugging

    def add_uri_if_needed(term_obj, document_date):
        """
        Helper function to add a URI to a term object if it doesn't already have one.

        Args:
        term_obj (dict): The term object.
        document_date (str): The date from the parent document.
        """
        term_type = term_obj.get('type')
        if term_type != 'term':
            # Query the VIAF API for the term
            add_viaf_uris(term_obj, term_type, document_date, debug_responses)

            # Generate a Rotunda URI for the term   
            term_obj['uri'] = generate_uri(term_obj['term'])

    # Loop through each document in the JSON data
    for document in tqdm(json_data['documents'], desc=f"Generating URIs for {args.modify_json_file}", unit="doc", position=args.position):
        document_date = document.get('dates', {}).get('date-from', '')[:4]  # Use only the year
        
        # Add URIs to the authors, recipients, and location
        for author in document['authors']:
            author = add_viaf_uris(author, 'person', document_date, debug_responses)
            author['uri'] = generate_uri(author['name'])

        for recipient in document['recipients']:
            recipient = add_viaf_uris(recipient, 'person', document_date, debug_responses)
            recipient['uri'] = generate_uri(recipient['name'])

        if document['location']:
            document['location'] = add_viaf_uris(document['location'], 'place', document_date, debug_responses)
            document['location']['uri'] = generate_uri(document['location']['name'])

        # Add URIs to the indexing terms
        for term in document['indexing']:
            add_uri_if_needed(term, document_date)
            if 'midsub' in term:
                add_uri_if_needed(term['midsub'], document_date)
            if 'sub' in term:
                add_uri_if_needed(term['sub'], document_date)

    # Save the debug responses to a JSON file
    debug_file_path = os.path.join(os.path.dirname(args.modify_json_file), f"debug_viaf_responses_{os.path.basename(args.modify_json_file)}")
    with open(debug_file_path, 'w') as debug_file:
        json.dump(debug_responses, debug_file, indent=4)
    
    return json_data

if __name__ == '__main__':
    # Parse the command-line arguments
    parser = argparse.ArgumentParser(description='Classify JSON terms.')
    parser.add_argument('modify_json_file', help='Path to the JSON file')
    parser.add_argument('position', type=int, help='Position for tqdm progress bar')
    args = parser.parse_args()

    # Load the JSON data
    with open(args.modify_json_file, 'r') as f:
        json_data = json.load(f)

    # Add URIs to the JSON data
    updated_json_data = add_uris_to_json(json_data)

    # Write the updated JSON data to a file
    with open(args.modify_json_file, 'w') as f:
        json.dump(updated_json_data, f, indent=4)

    print(f"URIs written to {args.modify_json_file}")
