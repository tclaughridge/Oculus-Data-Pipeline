# Oculus Data Pipeline
 
Authored by Thomas Laughridge || tclaughridge@virginia.edu

A data pipeline built for Rotunda Digital Publishing which ingests documents in XML format, converts to JSON, classifies document terms using the OpenAI API, and inserts the resulting data into a Neo4j Graph Database.

## Installation & Configuration

** Optionally, this entire pipeline can be executed within a Python virtual environment to prevent dependency issues with packages. Python beginners should probably disregard this step, but for more information, refer to: https://docs.python.org/3/library/venv.html

To begin, clone this repository onto your system. Open a terminal and navigate to the directory where the local repository is stored.

Once in the correct directory, install the required dependencies by running the following command:

```zsh
pip install -r requirements.txt
```
Next, open the file ```run_pipeline.py``` in your preferred text editor. You will see a section marked ```CONFIGURATION``` at the top of the file. Here you can modify the parameters of the script.

As a quick walkthrough - the first parameter, ```max_concurrent```, is the maximum number of files that can be concurrently processed by the pipeline. By default this is set to ```3```, and due to limitations on the number of API calls allowed at a time, it should probably not be raised much higher. **This could be subject to change in the future as API features are improved.

The next section, labeled **OpenAI**, contains three elements.

The first is the ```api_key```. By default, this is set to an internal environment variable. To link the API to your own account, you can either set up your own .env file, or you can simply replace the current value with your own key. Ex: ```api_key = 'abc123'```. This key can be created/found under the OpenAI API Dashboard: https://platform.openai.com/api-keys

The second paremeter is the ```model``` which the API will use. By default, this is set to ```gpt-4o-mini```. This should be updated to the most speed/cost/accuracy efficient model at the time you are using the pipeline.

The next section, labeled **Neo4j**, contains the identifier and login information for a Neo4j database. These settings: ```NEO4J_URI```, ```NEO4J_USER```, and ```NEO4J_PASSWORD``` will need to be modified depending on the variant and location of your database. Refer to https://neo4j.com/docs/operations-manual/current/configuration/ for more info.

Finally, the last parameter is the ```script_paths```. These can be left alone as long as all scripts included in the pipeline *remain in the same directory* together.

## Running the Pipeline

To execute the pipeline, run the following command in the terminal:
```zsh
python3 run_pipeline.py path/to/directory example.xml
```
Substituting the two arguments ```path/to/directory``` and ```example.xml``` with the location and file name of your XML files.

The pipeline can also handle multiple xml files concurrently. Simply add more filename arguments to the end of the command:
```zsh
python3 run_pipeline.py path/to/directory example.xml example2.xml example3.xml
```

Additionally, leaving the filename arguments blank will prompt the pipeline to process every XML file in the specified directory:
```zsh
python3 run_pipeline.py path/to/directory
```

This master command will run each script in the pipeline sequentially, going from an XML file to insertion into the database with no manual intermediate steps required.

If necessary, each pipeline script can also be run individually, taking the following arguments:

1. ```xml_to_json.py``` converts an XML file to a JSON
   
   ```zsh
   python3 xml_to_json.py input_file.xml output_file_name.json
   ```
2. ```json_classification.py``` classifies terms in the JSON using the OpenAI API

   ```zsh
   python3 json_classification.py input_file.json api_key model --test-mode=False
   ```
3. ```generate_uri.py``` generates URIs for all classified terms
   
   ```zsh
   python3 generate_uri.py input_file.json
   ```
4. ```json_to_db.py``` inserts the modified JSON into the Neo4j database
   
   ```zsh
   python3 json_to_db.py NEO4J_URI NEO4J_USER NEO4J_PASSWORD input_file.json
   ```

## Making Modifications

Any modifications made to this pipeline should be done on a new branch of the repository.

Inevitably, changes will have to be made in the future to support varying input and output formats/needs.

You may find these references helpful when modifying the pipeline:

- OpenAI Batch API Documentation: https://platform.openai.com/docs/guides/batch/overview
- Neo4j Graph DB Documentation: https://neo4j.com/docs/
- VIAF Query API Documentation: https://www.oclc.org/developer/api/oclc-apis/viaf/authority-cluster.en.html
