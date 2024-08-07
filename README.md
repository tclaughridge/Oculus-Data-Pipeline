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

Next, open the ```xml_to_json.py``` and ```json_to_db.py``` files in your preferred text editor. Optionally, to configure the scripts, modify the variables found in the sections marked ```Configuration```. This can also be done dynamically each time the scripts are run (see below).

## Running the Pipeline

To execute the first script, which converts the input XML file to a JSON, run the following command in the terminal:

```zsh
python xml_to_json.py
```

The script will ask if you would like to use the default configuration, or customize. Press ```y``` to continue with the preset configuration (which you are able to modify inside the script), or press ```n``` to enter your own paramenters. If you choose to do this the terminal window will prompt you to enter the script parameters here.
