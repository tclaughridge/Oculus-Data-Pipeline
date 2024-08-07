import os
import json
import re
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

def classify_terms(terms, api_key, model, batch_size):
    # Prepare the batch request for the OpenAI API
    term_to_label = {}
    word_count = 0

    responses = []

    for i in tqdm(range(0, len(terms), batch_size), desc="Classifying terms", unit="batch"):
        batch_terms = terms[i:i+batch_size]

        # Remove known entities from the batch
        for term in batch_terms:
            word_count += len(term.split())
            if normalize_term(term) in known_entities:
                batch_terms.remove(term)
        
        # Send the request to the OpenAI API
        client = OpenAI(api_key=api_key,)
        response = client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": "You are an NER system that classifies terms into PERSON, PLACE, ORGANIZATION, or TERM. Please look at each provided term in the list, and return it in the format {term: 'example_term', classification: 'example_class'}. For example, if an input term is 'Thomas Jefferson', you should return {term: 'Thomas Jefferson', classification: 'PERSON'}. Additionally, please only differentiate terms by line. 'Aberdeen, Scotland' should be one term, as it occupies one line. Please output in JSON format."
                },
                {
                    "role": "user",
                    "content": "Classify the following terms:\n\n" + "\n".join(batch_terms)
                }
            ],
            model=model,
            response_format={ "type": "json_object" }
        )

        # Parse the response
        try:
            response_text = response.choices[0].message.content.strip()

            response_data = json.loads(response_text)
            classifications = response_data.get("terms", [])
            if not isinstance(classifications, list):
                raise ValueError("Expected a list of dictionaries")

            batch_term_to_label = {item['term']: item['classification'] for item in classifications}
            term_to_label.update(batch_term_to_label)

            # Save the response for analysis
            responses.append({
                "batch_terms": batch_terms,
                "response": response_text
            })

            # Update global known entities
            for term, label in batch_term_to_label.items():
                if label in ['PERSON', 'PLACE', 'ORGANIZATION']:
                    known_entities[normalize_term(term)] = label

        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error parsing response: {e}")
            responses.append({
                "batch_terms": batch_terms,
                "response": response_text,
                "error": str(e)
            })
        
    # Save the responses to a file
    with open('openai_response.json', 'w') as json_file:
        json.dump(responses, json_file, indent=4)

    return term_to_label, word_count

def update_json_with_classifications(json_data, term_to_label):
    for document in json_data['documents']:
        for author in document['authors']:
            normalized_name = normalize_term(author['name'])
            author['name'] = convert_name(author['name'])
            if normalized_name in term_to_label:
                author['type'] = term_to_label[normalized_name].lower()

        for recipient in document['recipients']:
            normalized_name = normalize_term(recipient['name'])
            recipient['name'] = convert_name(recipient['name'])
            if normalized_name in term_to_label:
                recipient['type'] = term_to_label[normalized_name].lower()

        if document['location']:
            location_name = document['location']['name']
            normalized_location = normalize_term(location_name)
            if normalized_location in term_to_label:
                document['location']['type'] = term_to_label[normalized_location].lower()

        updated_terms = []
        for term in document['indexing']:
            main_term = term.get('main', "")
            midsub_term = term.get('midsub', "")
            sub_term = term.get('sub', "")

            main_label = term_to_label.get(main_term, 'TERM')
            midsub_label = term_to_label.get(midsub_term, 'TERM') if midsub_term else 'TERM'
            sub_label = term_to_label.get(sub_term, 'TERM') if sub_term else 'TERM'

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

    parser = argparse.ArgumentParser(description='Classify JSON terms.')
    parser.add_argument('modify_json_file', help='Path to the JSON file')
    parser.add_argument('api_key', help='OpenAI API Key')
    parser.add_argument('model', help='OpenAI Model')
    parser.add_argument('batch_size', help='API Call Batch Size')
    args = parser.parse_args()

    with open(args.modify_json_file, 'r') as f:
        json_data = json.load(f)

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

    term_to_label, word_count = classify_terms(list(terms_to_classify), args.api_key, args.model, int(args.batch_size))

    updated_json_data = update_json_with_classifications(json_data, term_to_label)
    
    with open(args.modify_json_file, 'w') as f:
        json.dump(updated_json_data, f, indent=4)

    print(f"Classified JSON data has been written to {args.modify_json_file}")
    print(f"Total terms processed: {word_count}")