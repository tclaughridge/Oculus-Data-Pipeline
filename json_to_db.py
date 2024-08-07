import json
import argparse
from tqdm import tqdm
from neo4j import GraphDatabase

def create_document(tx, document):
    query = """
    MERGE (d:Document {documentID: $documentID})
    ON CREATE SET d.title = $documentTitle, d.volumeInfo = $volumeInfo, d.publisher = $publisher, d.publicationName = $publicationName, d.seriesName = $seriesName
    """
    tx.run(query, documentID=document['documentID'], documentTitle=document['documentTitle'], 
           volumeInfo=document['projectInfo']['volumeInfo'], publisher=document['projectInfo']['publisher'], 
           publicationName=document['projectInfo']['publicationName'], seriesName=document['projectInfo']['seriesName'])

def create_person(tx, person):
    query = """
    MERGE (p:Person {name: $name, uri: $uri})
    """
    tx.run(query, name=person['term'], uri=person['uri'])

def create_place(tx, place):
    query = """
    MERGE (p:Place {name: $name, uri: $uri})
    """
    tx.run(query, name=place['term'], uri=place['uri'])

def create_organization(tx, organization):
    query = """
    MERGE (o:Organization {name: $name, uri: $uri})
    """
    tx.run(query, name=organization['term'], uri=organization['uri'])

def create_term(tx, term):
    query = """
    MERGE (t:Term {term: $term})
    """
    tx.run(query, term=term['term'])

def create_date(tx, date):
    query = """
    MERGE (d:Date {date: $date})
    """
    tx.run(query, date=date)

def create_relationship(tx, document_id, entity, role):
    query = f"""
    MATCH (d:Document {{documentID: $documentID}})
    MATCH (e {{uri: $uri}})
    MERGE (e)-[:{role.upper()}]->(d)
    """
    tx.run(query, documentID=document_id, uri=entity['uri'])

def relate_index_term(tx, document_id, term):
    query = """
    MATCH (d:Document {documentID: $documentID})
    MATCH (t:Term {term: $term})
    MERGE (d)-[:HAS_TERM]->(t)
    """
    tx.run(query, documentID=document_id, term=term['term'])

def relate_sub_term(tx, parent_term, sub_term, relation_type, parent_type):
    query = f"""
    MATCH (p:{parent_type} {{term: $parent_term}})
    MERGE (s:Term {{term: $sub_term}})
    MERGE (p)-[:{relation_type.upper()}]->(s)
    """
    tx.run(query, parent_term=parent_term, sub_term=sub_term['term'])

def relate_date(tx, document_id, date, role):
    query = f"""
    MATCH (d:Document {{documentID: $documentID}})
    MATCH (dt:Date {{date: $date}})
    MERGE (d)-[:{role.upper()}]->(dt)
    """
    tx.run(query, documentID=document_id, date=date)

def import_data(json_data, driver):
    with driver.session() as session:
        for document in tqdm(json_data['documents'], desc="Importing data to Neo4j", unit="doc"):
            session.execute_write(create_document, document)
            
            for author in document['authors']:
                session.execute_write(create_person, {'term': author['name'], 'uri': author['uri']})
                session.execute_write(create_relationship, document['documentID'], {'uri': author['uri']}, "author")
            
            for recipient in document['recipients']:
                session.execute_write(create_person, {'term': recipient['name'], 'uri': recipient['uri']})
                session.execute_write(create_relationship, document['documentID'], {'uri': recipient['uri']}, "recipient")
            
            if document['location']:
                session.execute_write(create_place, {'term': document['location']['name'], 'uri': document['location']['uri']})
                session.execute_write(create_relationship, document['documentID'], {'uri': document['location']['uri']}, "location")
            
            if 'date-from' in document['dates'] and document['dates']['date-from']:
                session.execute_write(create_date, document['dates']['date-from'])
                session.execute_write(relate_date, document['documentID'], document['dates']['date-from'], "DATE_FROM")

            if 'date-to' in document['dates'] and document['dates']['date-to']:
                session.execute_write(create_date, document['dates']['date-to'])
                session.execute_write(relate_date, document['documentID'], document['dates']['date-to'], "DATE_TO")
            
            for term in document['indexing']:
                if term['type'] == 'person':
                    session.execute_write(create_person, term)
                    session.execute_write(create_relationship, document['documentID'], term, "HAS_PERSON")
                    parent_type = "Person"
                elif term['type'] == 'place':
                    session.execute_write(create_place, term)
                    session.execute_write(create_relationship, document['documentID'], term, "HAS_PLACE")
                    parent_type = "Place"
                elif term['type'] == 'organization':
                    session.execute_write(create_organization, term)
                    session.execute_write(create_relationship, document['documentID'], term, "HAS_ORGANIZATION")
                    parent_type = "Organization"
                else:
                    session.execute_write(create_term, term)
                    session.execute_write(relate_index_term, document['documentID'], term)
                    parent_type = "Term"

                if 'midsub' in term:
                    session.execute_write(create_term, term['midsub'])
                    session.execute_write(relate_sub_term, term['term'], term['midsub'], "midsub", parent_type)
                
                if 'sub' in term:
                    session.execute_write(create_term, term['sub'])
                    session.execute_write(relate_sub_term, term['term'], term['sub'], "sub", parent_type)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Classify JSON terms.')
    parser.add_argument('NEO4J_URI', help='Neo4j URI')
    parser.add_argument('NEO4J_USER', help='Neo4j user')
    parser.add_argument('NEO4J_PASSWORD', help='Neo4j password')
    parser.add_argument('json_file', help='Path to the JSON file')
    args = parser.parse_args()

    with open(args.json_file, 'r') as f:
        json_data = json.load(f)

    # Connect to the Neo4j database
    driver = GraphDatabase.driver(args.NEO4J_URI, auth=(args.NEO4J_USER, args.NEO4J_PASSWORD))

    # Import the JSON data into Neo4j
    import_data(json_data, driver)

    driver.close()