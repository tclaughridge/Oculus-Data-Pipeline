import hashlib
import json
import argparse
import requests
from tqdm import tqdm
from requests.exceptions import RequestException
from tenacity import retry, stop_after_attempt, wait_fixed

# Retry settings: 5 attempts with a 2-second wait between each
@retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
def query_viaf_cluster(keyword):
    """
    Query the VIAF AutoSuggest API for matching terms

    Args:
    keyword (str): The keyword to search for
    """
    url = "https://www.viaf.org/viaf/AutoSuggest"
    params = {
        "query": keyword,
        "index": 'all',
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()  # Raise an exception for HTTP errors

    return response.json()

def generate_uri(input_string):
    """
    Generate a URI from a string by hashing it and taking the first 8 characters

    Args:
    input_string (str): The string to hash
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

def process_viaf_response(response):
    """
    Process the VIAF response and return relevant fields

    Args:
    response (dict): The response from the VIAF API
    """
    if response and 'result' in response and response['result']:
        records = []
        r = response['result'][0]
        if r is None:
            return None
        records.append({
            'viaf': r.get('viafid') or r.get('recordID'),
            'lc': r.get('lc') or r.get('lcid'),
        })
        return records
    return None

def add_viaf_uris(term_obj):
    """
    Add VIAF URIs to the JSON data

    Args:
    term_obj (dict): The term object
    """
    # Check if the 'term' key exists in the term_obj
    term_value = term_obj.get('term') or term_obj.get('name')
    if not term_value:
        print(f"Warning: No 'term' or 'name' key found in term_obj: {term_obj}")
        return term_obj

    try:
        viaf_response = query_viaf_cluster(term_value)
        if viaf_response:
            viaf_records = process_viaf_response(viaf_response)
            if viaf_records:
                term_obj['viaf'] = viaf_records[0].get('viaf')
                term_obj['lc'] = viaf_records[0].get('lc')
    except RequestException as e:
        print(f"Error querying VIAF API for term '{term_value}': {e}")
    
    return term_obj


def add_uris_to_json(json_data, position):
    """
    Add URIs to the JSON data
    
    Args:
    json_data (dict): The JSON data
    position (int): The position for the tqdm progress bar
    """
    def add_uri_if_needed(term_obj):
        """
        Helper function to add a URI to a term object if it doesn't already have one

        Args:
        term_obj (dict): The term object
        """
        term_type = term_obj.get('type')
        if term_type != 'term':
            # Query the VIAF API for the term
            add_viaf_uris(term_obj)

            # Generate a Rotunda URI for the term   
            term_obj['uri'] = generate_uri(term_obj['term'])

    # Loop through each document in the JSON data
    for document in tqdm(json_data['documents'], desc=f"Generating URIs for {args.modify_json_file}", unit="doc", position=position):
        # Add URIs to the authors, recipients, and location
        for author in document['authors']:
            author = add_viaf_uris(author)
            author['uri'] = generate_uri(author['name'])

        for recipient in document['recipients']:
            recipient = add_viaf_uris(recipient)
            recipient['uri'] = generate_uri(recipient['name'])

        if document['location']:
            document['location'] = add_viaf_uris(document['location'])
            document['location']['uri'] = generate_uri(document['location']['name'])

        # Add URIs to the indexing terms
        for term in document['indexing']:
            add_uri_if_needed(term)
            if 'midsub' in term:
                add_uri_if_needed(term['midsub'])
            if 'sub' in term:
                add_uri_if_needed(term['sub'])

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
    updated_json_data = add_uris_to_json(json_data, args.position)

    # Write the updated JSON data to a file
    with open(args.modify_json_file, 'w') as f:
        json.dump(updated_json_data, f, indent=4)

    print(f"URIs written to {args.modify_json_file}")