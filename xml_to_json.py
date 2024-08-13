import xml.etree.ElementTree as ET
import json
import re
import os
import argparse

def normalize_term(term):
    # Normalize the term for consistent comparison
    return re.sub(r'\s+', ' ', term).strip().lower()

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

    for document in root.findall('document'):
        authors = [{'name': author.text} for author in document.findall('./authors/author')]
        recipients = [{'name': recipient.text} for recipient in document.findall('./recipients/recipient')]
        
        location = document.find('./location/placeName')
        location_name = None
        if location is not None:
            location_name = location.text.strip()
        
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
            'location': {'name': location_name} if location is not None else None,
            'repositories': [repository.text for repository in document.findall('./repositories/repository')],
            'indexing': terms
        }
        documents.append(doc)

    json_data = {
        'documents': documents,
    }
    
    return json_data

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert XML to JSON.')
    parser.add_argument('input_file', help='Path to the input XML file')
    parser.add_argument('output_file', help='Name of the output JSON file')
    args = parser.parse_args()

    # Determine the directory of the script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Define the data directory within the script's directory
    data_dir = os.path.join(script_dir, 'data')

    # Create the data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)

    # Construct the output file path within the data directory
    output_path = os.path.join(data_dir, args.output_file)

    # Convert the XML to JSON
    json_data = parse_xml_to_json(args.input_file)
    
    # Write to the output file in the data directory
    with open(output_path, 'w') as f:
        json.dump(json_data, f, indent=4)

    print(f"JSON data has been written to {output_path}")
