import xml.etree.ElementTree as ET
import os
import json
import hashlib
import re
from tqdm import tqdm
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# CONFIGURATION ================================================================

xml_file = '/Users/tclaughridge/Downloads/test.xml'
output_json_file = 'output2.json'
save_json = True

api_key = os.getenv("OPENAI_API_KEY")
model = 'gpt-4o-mini'
batch_size = 300

# ==============================================================================

def generate_uri(input_string):
    # Generate a URI from a string by hashing it and taking the first 8 characters
    s = input_string.lower().replace(' ', '').replace(',', '')
    byte_string = s.encode('utf-8')
    hash_object = hashlib.sha256(byte_string)
    hex_digest = hash_object.hexdigest()
    hash_int = int(hex_digest[:8], 16) % 100000000
    return 'r' + str(hash_int)

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

def classify_terms(terms):
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
                    "content": "You are an NER system that classifies terms into PERSON, GPE (place), ORG (organization) or TERM. Please look at each provided term in the list, and return it in the format {term: 'example_term', classification: 'example_class'}. For example, if an input term is 'Thomas Jefferson', you should return {term: 'Thomas Jefferson', classification: 'PERSON'}. Additionally, please only differentiate terms by line. 'Aberdeen, Scotland' should be one term, as it occupies one line. Please output in JSON format."
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
                if label in ['PERSON', 'GPE', 'ORG']:
                    known_entities[normalize_term(term)] = label

        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error parsing response: {e}")
            responses.append({
                "batch_terms": batch_terms,
                "response": response_text,
                "error": str(e)
            })
        
    # Save the responses to a file
    if save_json:
        with open('openai_response.json', 'w') as json_file:
            json.dump(responses, json_file, indent=4)

    return term_to_label, word_count


def collect_terms_from_xml(document):
    terms = []
    unique_terms = set()

    for index_term in document.findall('./indexing/indexTerm'):
        main_term = index_term.find('main').text if index_term.find('main') is not None else ""
        midsub_term = index_term.find('midsub').text if index_term.find('midsub') is not None else ""
        sub_term = index_term.find('sub').text if index_term.find('sub') is not None else ""

        # Remove content within parentheses
        main_term = re.sub(r'\(.*?\)', '', main_term).strip()
        midsub_term = re.sub(r'\(.*?\)', '', midsub_term).strip() if midsub_term else ""
        sub_term = re.sub(r'\(.*?\)', '', sub_term).strip() if sub_term else ""
    
        # Duplicate removal
        term_tuple = (main_term, midsub_term, sub_term)
        if term_tuple not in unique_terms:
            unique_terms.add(term_tuple)
            terms.append({'main': main_term, 'midsub': midsub_term, 'sub': sub_term})

    return terms

def collect_terms_from_dict(document):
    terms = []
    for index_term in document['indexing']:
        main_term = index_term.get('term', "")
        midsub_term = index_term.get('midsub', {}).get('term', "")
        sub_term = index_term.get('sub', {}).get('term', "")

        terms.append((main_term, midsub_term, sub_term))
    return terms

def parse_xml_to_json(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    documents = []
    all_terms = []

    # First pass: Collect known entities and terms
    for document in root.findall('document'):
        authors = [{'name': author.text, 'uri': generate_uri(author.text)} for author in document.findall('./authors/author')]
        recipients = [{'name': recipient.text, 'uri': generate_uri(recipient.text)} for recipient in document.findall('./recipients/recipient')]
        
        # Add authors and recipients to known entities
        for author in authors:
            known_entities[normalize_term(author['name'])] = 'PERSON'
            author['name'] = convert_name(author['name'])
        for recipient in recipients:
            known_entities[normalize_term(recipient['name'])] = 'PERSON'
            recipient['name'] = convert_name(recipient['name'])
        
        location = document.find('./location/placeName')
        if location is not None:
            location_name = location.text.strip()
            known_entities[normalize_term(location_name)] = 'GPE'
        
        terms = collect_terms_from_xml(document)
        all_terms.extend(terms)

        doc = {
            'documentID': document.find('documentID').text if document.find('documentID') is not None else None,
            'documentTitle': document.find('documentTitle').text if document.find('documentTitle') is not None else None,
            'projectInfo': {
                'publicationName': document.find('./projectInfo/publicationName').text if document.find('./projectInfo/publicationName') is not None else None,
                'seriesName': document.find('./projectInfo/seriesName').text if document.find('./projectInfo/seriesName') is not None else None,
                'volumeInfo': document.find('./projectInfo/volumeInfo').text if document.find('./projectInfo/volumeInfo') is not None else None,
                'publisher': document.find('./projectInfo/publisher').text if document.find('./projectInfo/publisher') is not None else None,
                'formats': [format_type.text for format_type in document.findall('./projectInfo/formats/type')]
            },
            'authors': authors,
            'recipients': recipients,
            'dates': {
                'date-from': document.find('./dates/date-from').text if document.find('./dates/date-from') is not None else None,
                'date-to': document.find('./dates/date-to').text if document.find('./dates/date-to') is not None else None
            },
            'location': {'name': location_name, 'uri': generate_uri(location_name)} if location is not None else None,
            'repositories': [repository.text for repository in document.findall('./repositories/repository')],
            'indexing': collect_terms_from_xml(document)
        }
        documents.append(doc)

    # Filter terms to classify only those not in known_entities
    terms_to_classify = []
    for term_group in all_terms:
        for term_type, term_value in term_group.items():
            if term_value and normalize_term(term_value) not in known_entities:
                terms_to_classify.append(term_value)

    # Classify terms
    term_to_label, word_count = classify_terms(terms_to_classify)

    # Second pass: Add classified terms to documents
    for document in documents:
        terms = document['indexing']

        document['indexing'] = []
        for term in terms:
            main_term = term.get('main', "")
            midsub_term = term.get('midsub', "")
            sub_term = term.get('sub', "")

            # Add labels to terms
            main_label = known_entities.get(normalize_term(main_term)) or term_to_label.get(main_term, 'TERM')
            midsub_label = known_entities.get(normalize_term(midsub_term)) or term_to_label.get(midsub_term, 'TERM') if midsub_term else 'TERM'
            sub_label = known_entities.get(normalize_term(sub_term)) or term_to_label.get(sub_term, 'TERM') if sub_term else 'TERM'

            # Create main term objects
            if main_label == 'PERSON':
                main_term_obj = {'term': convert_name(main_term), 'uri': generate_uri(main_term), 'type': 'person'}
            elif main_label == 'GPE':
                main_term_obj = {'term': main_term, 'uri': generate_uri(main_term), 'type': 'place'}
            elif main_label == 'ORG':
                main_term_obj = {'term': main_term, 'uri': generate_uri(main_term), 'type': 'organization'}
            else:
                main_term_obj = {'term': main_term, 'type': 'term'}

            # Create midsub term objects and attach to main terms
            if midsub_term:
                if midsub_label == 'PERSON':
                    midsub_term_obj = {'term': convert_name(midsub_term), 'uri': generate_uri(midsub_term), 'type': 'person'}
                    main_term_obj['midsub'] = midsub_term_obj
                elif midsub_label == 'GPE':
                    midsub_term_obj = {'term': midsub_term, 'uri': generate_uri(midsub_term), 'type': 'place'}
                    main_term_obj['midsub'] = midsub_term_obj
                elif midsub_label == 'ORG':
                    midsub_term_obj = {'term': midsub_term, 'uri': generate_uri(midsub_term), 'type': 'organization'}
                    main_term_obj['midsub'] = midsub_term_obj
                else:
                    midsub_term_obj = {'term': midsub_term, 'type': midsub_label.lower()}
                    main_term_obj['midsub'] = midsub_term_obj

            # Create sub term objects and attach to main terms
            if sub_term:
                if sub_label == 'PERSON':
                    sub_term_obj = {'term': convert_name(sub_term), 'uri': generate_uri(sub_term), 'type': 'person'}
                    main_term_obj['sub'] = sub_term_obj
                elif sub_label == 'GPE':
                    sub_term_obj = {'term': sub_term, 'uri': generate_uri(sub_term), 'type': 'place'}
                    main_term_obj['sub'] = sub_term_obj
                elif sub_label == 'ORG':
                    sub_term_obj = {'term': sub_term, 'uri': generate_uri(sub_term), 'type': 'organization'}
                    main_term_obj['sub'] = sub_term_obj
                else:
                    sub_term_obj = {'term': sub_term, 'type': sub_label.lower()}
                    main_term_obj['sub'] = sub_term_obj

            document['indexing'].append(main_term_obj)

    json_data = {
        'documents': documents
    }
    
    return json_data, word_count


if __name__ == "__main__":
    # Initialize global variables
    known_entities = {}

    # Customize Configuration
    config = input("Do you want to proceed with the default configuration? (y/n) ")

    if config.lower() == 'n':
        xml_file = input("Enter the path to the XML file: ")
        sj = input ("Do you want to save the JSON file? (y/n) ")

        if sj.lower() == 'y':
            output_json_file = input("Enter the path to the JSON file: ")
        else:
            save_json = False

        api_key = input("Enter your OpenAI API key: ")
        m = input("Proceed with the default model (GPT-4o)? (y/n) ")

        if m.lower() == 'n':
            model = input("Enter the model name: ")

        batch_size = int(input("Enter the API batch size: "))

    # Parse the XML file and convert to JSON
    json_data, word_count = parse_xml_to_json(xml_file)
    print(f"Total index terms processed: {word_count}")

    # Optionally save the JSON data to a file
    if save_json:
        with open(output_json_file, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)