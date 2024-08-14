import json
import os
import re
import time
import argparse
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

def load_mock_data(input_file, output_file):
    """
    Loads pre-existing data for testing purposes instead of making API calls

    Args:
    input_file (str): Path to the input file
    output_file (str): Path to the output file
    """
    input_dict = {}
    with open(input_file, 'r') as infile:
        for line in infile:
            entry = json.loads(line)
            custom_id = entry['custom_id']
            content = entry['body']['messages'][1]['content']
            input_dict[custom_id] = content

    output_dict = {}
    with open(output_file, 'r') as outfile:
        for line in outfile:
            entry = json.loads(line)
            custom_id = entry['custom_id']
            classification = json.loads(entry['response']['body']['choices'][0]['message']['content'])['classification']
            output_dict[custom_id] = classification

    matched_data = [{"custom_id": cid, "content": input_dict.get(cid, "Unknown"), "classification": output_dict.get(cid, "Unknown")} for cid in input_dict]
    
    return matched_data


def convert_name(name):
    """
    Convert a name from 'Last, First' to 'Title First Last', handling cases with titles, prefixes, and suffixes.

    Args:
    name (str): The name to convert

    Returns:
    str: The name in 'Title First Last' format
    """
    if ',' in name:
        parts = name.split(', ')
        if len(parts) == 2:
            last_name = parts[0]
            first_name_and_titles = parts[1]

            # Identify the title or prefix (e.g., "Baron", "marquis") if it exists
            titles = []
            title_keywords = ["Baron", "Sir", "Dr.", "Lord", "Dame", "Count", "Countess", "King", "Queen", "Prince", "Princess", "Duke", "Duchess", "marquis", "marchioness", "von", "de"]

            for keyword in title_keywords:
                if keyword in first_name_and_titles:
                    titles.append(keyword)
                    first_name_and_titles = first_name_and_titles.replace(keyword, '').strip()

            title_str = " ".join(titles)
            return f"{title_str} {first_name_and_titles} de {last_name}" if title_str else f"{first_name_and_titles} {last_name}"
    return name


def normalize_term(term):
    """
    Normalize the term for consistent comparison

    Args:
    term (str): The term to normalize
    """
    return re.sub(r'\s+', ' ', term).strip().lower()


def create_term_obj(term, label):
    """
    Create a term object with the term and label

    Args:
    term (str): The term
    label (str): The label (person, place, organization, term)
    """
    term_obj = {
        'term': convert_name(term) if label == 'person' else term,
        'type': label.lower()
    }
    return term_obj


def classify_terms(terms, api_key, model, test_mode=False):
    """
    Classify the terms using the OpenAI Batching API

    Args:
    terms (list): The list of terms to classify
    api_key (str): The OpenAI API Key
    model (str): The OpenAI model to use
    test_mode (bool): Whether to use existing files for testing instead of making API calls
    """
    
    # Use pre-existing data for testing if test_mode is enabled
    if test_mode:
        print("Test mode enabled, using existing files instead of making API calls.")
        matched_data = load_mock_data(f'data/batch_tasks_{base_filename}.jsonl', f'data/batch_results_{base_filename}.jsonl')
        return matched_data, len(matched_data)
    
    # Prepare the request for the OpenAI API
    client = OpenAI(api_key=api_key,)

    # Track number of requests sent to API
    request_count = 0

    tasks = []

    for index, term in enumerate(terms):
        # Known entities check
        if normalize_term(term) in known_entities:
            continue

        # Accumulate tasks
        task = {
            # API request parameters
            "custom_id": f"task-{index}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": model,
                "temperature": 0.1,
                "response_format": { 
                    "type": "json_object"
                },
                "messages": [
                    {
                        "role": "system",
                        "content": api_prompt
                    },
                    {
                        "role": "user",
                        "content": term
                    }
                ],
            }
        }
        tasks.append(task)

        # Increment request count based on # of tasks
        request_count += 1

    # Creating batch file
    with open(f'data/batch_tasks_{base_filename}.jsonl', 'w') as file:
        for obj in tasks:
            file.write(json.dumps(obj) + '\n')
    
    # Uploading batch file
    batch_file = client.files.create(
        file=open(f'data/batch_tasks_{base_filename}.jsonl', 'rb'),
        purpose="batch"
    )

    # Creating batch job
    try:
        batch_job = client.batches.create(
            input_file_id=batch_file.id,
            endpoint="/v1/chat/completions",
            completion_window="24h"
        )
        print(f"Batch sent to OpenAI API for ({base_filename}). Job ID: {batch_job.id}")
    except Exception as e:
        print(f"Error creating batch job: {e}")
        return

    # Await response
    print("Awaiting response...")
    while True:
        # Continually check the status of the batch job until complete
        batch_job = client.batches.retrieve(batch_job.id)
        if batch_job.status == "completed" or batch_job.status == "failed":
            print(f"Batch job {batch_job.status}.")
            break
        time.sleep(5)

    # Retrieve Results
    result_file_id = batch_job.output_file_id
    result = client.files.content(result_file_id).content

    with open(f"data/batch_results_{base_filename}.jsonl", 'wb') as file:
        file.write(result)

    # Process Results
    # Load the API input data
    input_dict = {}
    with open(f'data/batch_tasks_{base_filename}.jsonl', 'r') as infile:
        for line in infile:
            entry = json.loads(line)
            custom_id = entry['custom_id']
            # Extract the user's message content (this is the term to be classified)
            content = entry['body']['messages'][1]['content']
            input_dict[custom_id] = content

    # Load the API output data
    output_dict = {}
    with open(f'data/batch_results_{base_filename}.jsonl', 'r') as outfile:
        for line in outfile:
            entry = json.loads(line)
            custom_id = entry['custom_id']
            classification = json.loads(entry['response']['body']['choices'][0]['message']['content'])['classification']
            output_dict[custom_id] = classification

    # Match the input and output data based on custom_id
    matched_data = [{"custom_id": cid, "content": input_dict.get(cid, "Unknown"), "classification": output_dict.get(cid, "Unknown")} for cid in input_dict]
    
    return matched_data, request_count


def update_json(json_data, matched_data):
    """
    Update the JSON data with the classified terms

    Args:
    json_data (dict): The JSON data to update
    matched_data (dict): The matched data retrieved from the API
    """

    # Loop through each document in the JSON data
    for document in json_data['documents']:

        # Convert author and recipient names
        for author in document['authors']:
            author['name'] = convert_name(author['name'])

        for recipient in document['recipients']:
            recipient['name'] = convert_name(recipient['name'])
        
        # Loop through index terms
        updated_terms = []
        for term in document.get('indexing', []):
            # Extract main, midsub, and sub terms
            main_term = term.get('main', "")
            midsub_term = term.get('midsub', "")
            sub_term = term.get('sub', "")

            # Extract term strings from dictionaries if necessary
            main_term_str = main_term['term'] if isinstance(main_term, dict) else main_term
            midsub_term_str = midsub_term['term'] if isinstance(midsub_term, dict) else midsub_term
            sub_term_str = sub_term['term'] if isinstance(sub_term, dict) else sub_term

            # Normalize the terms
            normalized_main = normalize_term(main_term_str)
            normalized_midsub = normalize_term(midsub_term_str)
            normalized_sub = normalize_term(sub_term_str)

            # Assign labels from matched_data or fallback to 'TERM'
            if normalized_main in known_entities:
                # Use known entity classification if available
                main_label = known_entities[normalized_main]
            else:
                main_label = matched_data.get(normalized_main, {'type': 'TERM'})['type']
            midsub_label = matched_data.get(normalized_midsub, {'type': 'TERM'})['type'] if midsub_term_str else 'TERM'
            sub_label = matched_data.get(normalized_sub, {'type': 'TERM'})['type'] if sub_term_str else 'TERM'

            # Create main term object
            main_term_obj = create_term_obj(main_term_str, main_label)

            # Create midsub term object and attach to main term
            if midsub_term_str:
                main_term_obj['midsub'] = create_term_obj(midsub_term_str, midsub_label)

            # Create sub term object and attach to main term
            if sub_term_str:
                main_term_obj['sub'] = create_term_obj(sub_term_str, sub_label)

            updated_terms.append(main_term_obj)
        
        document['indexing'] = updated_terms

    return json_data



if __name__ == '__main__':
    # Initialize global variables
    known_entities = {}
    api_prompt = '''
    You are an NER system that classifies terms into PERSON, PLACE, ORGANIZATION, or TERM. Please look at each provided 
    term in the list, and return it in the format {classification: 'example_class'}. For example, if an input term is 
    'Thomas Jefferson', you should return {classification: 'PERSON'}. Additionally, please only differentiate terms by line. 
    'Aberdeen, Scotland' should be one term, as it occupies one line. Please output as a json object in the following format:

    {
        classification: string // A string describing the term as a PERSON, PLACE, ORGANIZATION, or TERM
    }
    '''

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Classify JSON terms.')
    parser.add_argument('modify_json_file', help='Path to the JSON file')
    parser.add_argument('api_key', help='OpenAI API Key')
    parser.add_argument('model', help='OpenAI Model')
    parser.add_argument('--test-mode', action='store_true', help='Use existing input and output files for testing without making API calls')
    args = parser.parse_args()

    base_filename = os.path.basename(os.path.splitext(args.modify_json_file)[0])

    # Load JSON data
    with open(f'{args.modify_json_file}', 'r') as f:
        json_data = json.load(f)
    
    # Initial pass to populate known entities
    for document in json_data['documents']:
        for author in document.get('authors', []):
            known_entities[normalize_term(author['name'])] = 'person'
        for recipient in document.get('recipients', []):
            known_entities[normalize_term(recipient['name'])] = 'person'
        if 'location' in document and document['location']:
            known_entities[normalize_term(document['location']['name'])] = 'place'

    # Extract terms to classify
    terms_to_classify = set()
    for document in json_data['documents']:
        for term in document['indexing']:
            main_term = term.get('main', "")
            midsub_term = term.get('midsub', "")
            sub_term = term.get('sub', "")
            
            if isinstance(main_term, str):
                terms_to_classify.add(main_term)
            if isinstance(midsub_term, str):
                terms_to_classify.add(midsub_term)
            if isinstance(sub_term, str):
                terms_to_classify.add(sub_term)

    # Classify terms
    term_to_label, request_count = classify_terms(list(terms_to_classify), args.api_key, args.model, args.test_mode)

    # Convert classified terms to dict
    term_to_label_dict = {
        normalize_term(item['content']): {'type': item['classification'].lower()} for item in term_to_label
    }

    # Update JSON data with classified terms
    updated_json_data = update_json(json_data, term_to_label_dict)
    
    # Write updated JSON data to file
    with open(f'data/{args.modify_json_file}', 'w') as f:
        json.dump(updated_json_data, f, indent=4)

    print(f"Classified JSON data has been written to {args.modify_json_file}. {request_count} total terms processed.")