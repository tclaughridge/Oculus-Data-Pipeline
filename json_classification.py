import json
import re
import time
import argparse
from tqdm import tqdm
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

def convert_name(name):
    # Convert from "Last, First" to "First Last"
    if ',' in name:
        parts = name.split(', ')
        if len(parts) == 2:
            return f"{parts[1]} {parts[0]}"
    return name

def normalize_term(term):
    # Normalize the term for consistent comparison
    return re.sub(r'\s+', ' ', term).strip().lower()

def create_term_obj(term, label):
    # Create a term object with the appropriate fields
    term_obj = {
        'term': convert_name(term) if label == 'PERSON' else term,
        'type': label.lower()
    }
    return term_obj

def classify_terms(terms, api_key, model):
    # Prepare the request for the OpenAI API
    client = OpenAI(api_key=api_key,)
    term_to_label = {}
    word_count = 0

    tasks = []

    for index, term in enumerate(terms):
        # Remove known entities from the batch
        word_count += len(term.split())
        if normalize_term(term) in known_entities:
            continue
        
        # Accumulate tasks
        task = {
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

    # Creating batch file
    with open('data/batch_tasks.jsonl', 'w') as file:
        for obj in tasks:
            file.write(json.dumps(obj) + '\n')
    
    # Uploading batch file
    batch_file = client.files.create(
        file=open('data/batch_tasks.jsonl', 'rb'),
        purpose="batch"
    )

    # Creating batch job
    batch_job = client.batches.create(
        input_file_id=batch_file.id,
        endpoint="/v1/chat/completions",
        completion_window="24h"
    )
    print(f"Batch sent to OpenAI API. Job ID: {batch_job.id}")

    # Await response
    print("Awaiting response...")
    while True:
        batch_job = client.batches.retrieve(batch_job.id)
        if batch_job.status == "completed" or batch_job.status == "failed":
            print(f"Batch job completed with status {batch_job.status}.")
            break
        time.sleep(5)

    # Retrieve Results
    result_file_id = batch_job.output_file_id
    result = client.files.content(result_file_id).content

    with open("data/batch_job_results.jsonl", 'wb') as file:
        file.write(result)
    
    # Map custom_id to terms from batch_tasks.jsonl
    term_mapping = {}
    with open('data/batch_tasks.jsonl', 'r') as file:
        for line in file:
            json_object = json.loads(line.strip())
            custom_id = json_object['custom_id']
            term = json_object['body']['messages'][1]['content']
            term_mapping[custom_id] = term
    
    # Loading data from saved file
    with open('data/batch_job_results.jsonl', 'r') as file:
        for line in file:
            json_object = json.loads(line.strip())
            custom_id = json_object['custom_id']
            term = term_mapping[custom_id]
            classification = json_object['response']['body']['choices'][0]['message']['content'].strip()

            try:
                classification_data = json.loads(classification)
                term_type = classification_data['classification']

                term_to_label[term] = {
                    'type': term_type,
                }

                # Update global known entities if applicable
                if term_type in ['PERSON', 'PLACE', 'ORGANIZATION']:
                    known_entities[normalize_term(term)] = term_type

            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error parsing response for term {term}: {e}")
                term_to_label[term] = {
                    'type': 'TERM',
                }

    return term_to_label, word_count

def update_json_with_classifications(json_data, term_to_label):
    for document in json_data['documents']:
        for author in document['authors']:
            normalized_name = normalize_term(author['name'])
            author['name'] = convert_name(author['name'])
            if normalized_name in term_to_label:
                author['type'] = term_to_label[normalized_name]['type'].lower()

        for recipient in document['recipients']:
            normalized_name = normalize_term(recipient['name'])
            recipient['name'] = convert_name(recipient['name'])
            if normalized_name in term_to_label:
                recipient['type'] = term_to_label[normalized_name]['type'].lower()

        if document['location']:
            location_name = document['location']['name']
            normalized_location = normalize_term(location_name)
            if normalized_location in term_to_label:
                document['location']['type'] = term_to_label[normalized_location]['type'].lower()

        updated_terms = []
        for term in document['indexing']:
            main_term = term.get('main', "")
            midsub_term = term.get('midsub', "")
            sub_term = term.get('sub', "")

            main_label = term_to_label.get(normalize_term(main_term), {'type': 'TERM'})['type']
            midsub_label = term_to_label.get(normalize_term(midsub_term), {'type': 'TERM'})['type'] if midsub_term else 'TERM'
            sub_label = term_to_label.get(normalize_term(sub_term), {'type': 'TERM'})['type'] if sub_term else 'TERM'

            # Create main term objects
            main_term_obj = create_term_obj(main_term, main_label)

            # Create midsub term objects and attach to main terms
            if midsub_term:
                main_term_obj['midsub'] = create_term_obj(midsub_term, midsub_label)

            # Create sub term objects and attach to main terms
            if sub_term:
                main_term_obj['sub'] = create_term_obj(sub_term, sub_label)

            updated_terms.append(main_term_obj)
        
        document['indexing'] = updated_terms

    return {'documents': json_data['documents']}


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

    parser = argparse.ArgumentParser(description='Classify JSON terms.')
    parser.add_argument('modify_json_file', help='Path to the JSON file')
    parser.add_argument('api_key', help='OpenAI API Key')
    parser.add_argument('model', help='OpenAI Model')
    parser.add_argument('batch_size', help='API Call Batch Size')
    args = parser.parse_args()

    with open(f'data/{args.modify_json_file}', 'r') as f:
        json_data = json.load(f)
    
    # Initial pass to populate known entities
    for document in json_data['documents']:
        for author in document.get('authors', []):
            known_entities[normalize_term(author['name'])] = 'PERSON'
        for recipient in document.get('recipients', []):
            known_entities[normalize_term(recipient['name'])] = 'PERSON'
        if 'location' in document and document['location']:
            known_entities[normalize_term(document['location']['name'])] = 'PLACE'

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

    term_to_label, word_count = classify_terms(list(terms_to_classify), args.api_key, args.model)

    updated_json_data = update_json_with_classifications(json_data, term_to_label)
    
    with open(f'data/{args.modify_json_file}', 'w') as f:
        json.dump(updated_json_data, f, indent=4)

    print(f"Classified JSON data has been written to {args.modify_json_file}. {word_count} total terms processed.")