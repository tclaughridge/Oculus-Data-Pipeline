import hashlib
import json

# CONFIGURATION ================================================================

modify_json_file = 'output.json'

# ==============================================================================

def generate_uri(input_string):
    # Generate a URI from a string by hashing it and taking the first 8 characters
    s = input_string.lower().replace(' ', '').replace(',', '')
    byte_string = s.encode('utf-8')
    hash_object = hashlib.sha256(byte_string)
    hex_digest = hash_object.hexdigest()
    hash_int = int(hex_digest[:8], 16) % 100000000
    return 'r' + str(hash_int)

def add_uris_to_json(json_data):
    def add_uri_if_needed(term_obj):
        term_type = term_obj.get('type')
        if term_type != 'term':
            term_obj['uri'] = generate_uri(term_obj['term'])

    for document in json_data['documents']:
        for author in document['authors']:
            author['uri'] = generate_uri(author['name'])

        for recipient in document['recipients']:
            recipient['uri'] = generate_uri(recipient['name'])

        if document['location']:
            document['location']['uri'] = generate_uri(document['location']['name'])

        for term in document['indexing']:
            add_uri_if_needed(term)
            if 'midsub' in term:
                add_uri_if_needed(term['midsub'])
            if 'sub' in term:
                add_uri_if_needed(term['sub'])

    return json_data

if __name__ == '__main__':
    # Customize Configuration
    config = input("Do you want to proceed with the default configuration? (y/n) ")

    if config.lower() == 'n':
        modify_json_file = input("Enter the path to the JSON file: ")

    with open(modify_json_file, 'r') as f:
        json_data = json.load(f)

    updated_json_data = add_uris_to_json(json_data)

    with open(modify_json_file, 'w') as f:
        json.dump(updated_json_data, f, indent=4)

    print(f"URIs written to {modify_json_file}")