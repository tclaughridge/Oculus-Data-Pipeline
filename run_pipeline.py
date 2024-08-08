import argparse
import subprocess
import os
from dotenv import load_dotenv

load_dotenv()

# CONFIGURATION ================================================================

# OpenAI
api_key = os.getenv("OPENAI_API_KEY")
model = 'gpt-4o-mini'
batch_size = 300

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

def run_pipeline(data_dir, xml_files):
    for xml_file in xml_files:
        print(f"Processing {xml_file}...")
        json_file = xml_file.replace('.xml', '.json')
        
        # Step 1: Translate XML to JSON
        result = subprocess.run(["python3", script_paths["xml_to_json"], os.path.join(data_dir, xml_file), os.path.join(data_dir, json_file)])
        if result.returncode != 0:
            print(f"Error in xml_to_json.py for file {xml_file}")
            continue

        # Step 2: Classify terms in JSON
        result = subprocess.run(["python3", script_paths["json_classification"], os.path.join(data_dir, json_file), api_key, model, str(batch_size)])
        if result.returncode != 0:
            print(f"Error in json_classification.py for file {json_file}")
            continue

        # Step 3: Generate URIs for classified terms
        result = subprocess.run(["python3", script_paths["generate_uri"], os.path.join(data_dir, json_file)])
        if result.returncode != 0:
            print(f"Error in generate_uri.py for file {json_file}")
            continue

        # Step 4: Insert JSON into Neo4j database
        result = subprocess.run(["python3", script_paths["json_to_db"], NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, os.path.join(data_dir, json_file)])
        if result.returncode != 0:
            print(f"Error in json_to_db.py for file {json_file}")
            continue

        print(f"Finished processing {xml_file}.")

    print("All files processed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run the data processing pipeline.')
    parser.add_argument('data_dir', help='Directory containing the XML files')
    parser.add_argument('xml_files', nargs='+', help='List of XML files to process')
    args = parser.parse_args()

    run_pipeline(args.data_dir, args.xml_files)
