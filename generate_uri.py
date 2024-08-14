import hashlib
import json
import argparse

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


def add_uris_to_json(json_data):
    """
    Add URIs to the JSON data
    
    Args:
    json_data (dict): The JSON data
    """
    def add_uri_if_needed(term_obj):
        """
        Helper function to add a URI to a term object if it doesn't already have one

        Args:
        term_obj (dict): The term object
        """
        term_type = term_obj.get('type')
        if term_type != 'term':
            term_obj['uri'] = generate_uri(term_obj['term'])

    # Loop through each document in the JSON data
    for document in json_data['documents']:
        # Add URIs to the authors, recipients, and location
        for author in document['authors']:
            author['uri'] = generate_uri(author['name'])

        for recipient in document['recipients']:
            recipient['uri'] = generate_uri(recipient['name'])

        if document['location']:
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
    args = parser.parse_args()

    # Load the JSON data
    with open(f'data/{args.modify_json_file}', 'r') as f:
        json_data = json.load(f)

    # Add URIs to the JSON data
    updated_json_data = add_uris_to_json(json_data)

    # Write the updated JSON data to a file
    with open(f'data/{args.modify_json_file}', 'w') as f:
        json.dump(updated_json_data, f, indent=4)

    print(f"URIs written to {args.modify_json_file}")