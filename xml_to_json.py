import xml.etree.ElementTree as ET
import json
import re

# CONFIGURATION ================================================================

xml_file = '/Users/tclaughridge/Downloads/test.xml'
output_json_file = 'output.json'

# ==============================================================================

def normalize_term(term):
    # Normalize the term for consistent comparison
    return re.sub(r'\s+', ' ', term).strip().lower()

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
    known_entities = {}
    all_terms = []

    for document in root.findall('document'):
        authors = [{'name': author.text} for author in document.findall('./authors/author')]
        recipients = [{'name': recipient.text} for recipient in document.findall('./recipients/recipient')]
        
        # Add authors and recipients to known entities
        for author in authors:
            known_entities[normalize_term(author['name'])] = 'PERSON'
        for recipient in recipients:
            known_entities[normalize_term(recipient['name'])] = 'PERSON'
        
        location = document.find('./location/placeName')
        location_name = None
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
            'location': {'name': location_name} if location is not None else None,
            'repositories': [repository.text for repository in document.findall('./repositories/repository')],
            'indexing': terms
        }
        documents.append(doc)

    json_data = {
        'documents': documents,
        'known_entities': known_entities
    }
    
    return json_data

if __name__ == '__main__':
    # Customize Configuration
    config = input("Do you want to proceed with the default configuration? (y/n) ")

    if config.lower() == 'n':
        xml_file = input("Enter the path to the input XML file: ")
        output_json_file = input("Enter the path to the output JSON file: ")

    json_data = parse_xml_to_json(xml_file)
    
    with open(output_json_file, 'w') as f:
        json.dump(json_data, f, indent=4)

    print(f"JSON data has been written to {output_json_file}")