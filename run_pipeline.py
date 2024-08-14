import argparse
import subprocess
import os
from concurrent.futures import ProcessPoolExecutor
from dotenv import load_dotenv

load_dotenv()

# CONFIGURATION ================================================================

# Concurrency
max_concurrent = 3

# OpenAI
api_key = os.getenv("OPENAI_API_KEY")
model = 'gpt-4o-mini'

# Neo4j
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# Define paths to scripts
script_paths = {
    "xml_to_json": "xml_to_json.py",
    "json_classification": "json_classification.py",
    "generate_uri": "generate_uri.py",
    "json_to_db": "json_to_db.py"
}

# ==============================================================================

def process_file_pipeline(xml_file, xml_dir, data_dir):
    print(f"Processing {xml_file}...")

    # Construct the full path to the XML file
    xml_file_path = os.path.join(xml_dir, xml_file)

    # Determine the output JSON file name and path
    json_file = xml_file.replace('.xml', '.json')
    json_file_path = os.path.join(data_dir, json_file)
    
    # Step 1: Translate XML to JSON
    result = subprocess.run(["python3", script_paths["xml_to_json"], xml_file_path, json_file])
    if result.returncode != 0:
        print(f"Error in xml_to_json.py for file {xml_file}")
        return
    
    # Step 2: Classify terms in JSON
    result = subprocess.run(["python3", script_paths["json_classification"], json_file_path, api_key, model])
    if result.returncode != 0:
        print(f"Error in json_classification.py for file {json_file}")
        return
    
    # Step 3: Generate URIs for classified terms
    result = subprocess.run(["python3", script_paths["generate_uri"], json_file_path])
    if result.returncode != 0:
        print(f"Error in generate_uri.py for file {json_file}")
        return

    # Step 4: Insert JSON into Neo4j database
    result = subprocess.run(["python3", script_paths["json_to_db"], NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, json_file_path])
    if result.returncode != 0:
        print(f"Error in json_to_db.py for file {json_file}")
        return

    print(f"Finished processing {xml_file}.")

def run_pipeline(xml_dir, xml_files):
    # Determine the directory of the script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Define the data directory within the script's directory
    data_dir = os.path.join(script_dir, 'data')

    # Ensure the data directory exists
    os.makedirs(data_dir, exist_ok=True)

    # Use ProcessPoolExecutor to run the entire pipeline concurrently for each XML file
    with ProcessPoolExecutor(max_workers=max_concurrent) as executor:
        futures = [executor.submit(process_file_pipeline, xml_file, xml_dir, data_dir) for xml_file in xml_files]
        
        for future in futures:
            try:
                future.result()  # Wait for each process to finish and check for exceptions
            except Exception as exc:
                print(f"An error occurred: {exc}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run the data processing pipeline.')
    parser.add_argument('xml_dir', help='Directory containing the XML files')
    parser.add_argument('xml_files', nargs='+', help='List of XML files to process')
    args = parser.parse_args()

    run_pipeline(args.xml_dir, args.xml_files)
