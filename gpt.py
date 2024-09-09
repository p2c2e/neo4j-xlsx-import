import pandas as pd
import json
import argparse
import csv
import os

def process_entities(sheet, entities_config, import_folder):
    node_files = []
    for entity_config in entities_config:
        for entity_name, properties_list in entity_config.items():
            # Replace spaces with underscores in entity name
            entity_name_sanitized = entity_name.replace(' ', '_')

            # If properties_list is not a list, convert it to a list
            if not isinstance(properties_list, list):
                properties_list = [properties_list]

            # Initialize the DataFrame to store entity data
            entity_df = pd.DataFrame()

            for properties in properties_list:
                # Extract column for entity ID
                entity_column = properties['column']

                # Skip if the entity column is empty
                if sheet[entity_column].isnull().all():
                    continue

                # Create a temporary DataFrame for the current entity column
                temp_df = pd.DataFrame()
                temp_df[f'{entity_name_sanitized}:ID'] = sheet[entity_column]

                # Add properties columns if they exist
                for prop_name, prop_col in properties.items():
                    if prop_name != 'column':
                        temp_df[prop_name] = sheet[prop_col]

                # Remove rows where the entity ID is empty
                temp_df = temp_df[temp_df[f'{entity_name_sanitized}:ID'].notna()]

                # Merge the temporary DataFrame into the main entity DataFrame
                entity_df = pd.concat([entity_df, temp_df], ignore_index=True)

            # Add the :LABEL column
            entity_df[':LABEL'] = entity_name_sanitized

            # Fill missing property columns with empty strings
            for properties in properties_list:
                for prop_name in properties:
                    if prop_name != 'column' and prop_name not in entity_df.columns:
                        entity_df[prop_name] = ""

            # Remove duplicate rows based on the ":ID" column
            entity_df = entity_df.drop_duplicates(subset=[f'{entity_name_sanitized}:ID'], keep='first')

            # Save to CSV with quoting applied by pandas
            filename = os.path.join(import_folder, f'{entity_name_sanitized}.csv')
            entity_df.to_csv(filename, index=False, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            node_files.append(filename)

    return node_files

def process_relations(sheet, relations_config, import_folder):
    relationship_files = []
    for relation_config in relations_config:
        for relation_name, relation_details in relation_config.items():
            from_column = relation_details['from']
            to_column = relation_details['to']
            properties = relation_details.get('properties', {})

            # Skip if the from or to columns are empty
            if sheet[from_column].isnull().all() or sheet[to_column].isnull().all():
                continue

            # Create the additional CSV file with properties
            additional_df = pd.DataFrame()
            additional_df[':START_ID'] = sheet[from_column]
            additional_df[':END_ID'] = sheet[to_column]
            additional_df[':TYPE'] = relation_name  # No double-quote for :TYPE

            # Remove rows where from/to IDs are empty
            additional_df = additional_df[additional_df[':START_ID'].notna() & additional_df[':END_ID'].notna()]

            # Add properties to the additional CSV
            for prop_name, prop_col in properties.items():
                additional_df[prop_name] = sheet[prop_col]

            # Remove duplicate rows based on the ":START_ID" and ":END_ID" columns
            additional_df = additional_df.drop_duplicates(subset=[':START_ID', ':END_ID'], keep='first')

            # Replace spaces with underscores in column names and filenames
            from_col_sanitized = from_column.replace(' ', '_')
            to_col_sanitized = to_column.replace(' ', '_')

            # Save the additional CSV file with the format "{from_col}_{to_col}.csv"
            additional_filename = os.path.join(import_folder, f'{from_col_sanitized}_{to_col_sanitized}.csv')
            additional_df.to_csv(additional_filename, index=False, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            relationship_files.append(additional_filename)

    return relationship_files

def process_xlsx_file(xlsx_file, config_file, data_folder, import_folder):
    # Convert relative paths to absolute paths
    data_folder = os.path.abspath(data_folder)
    import_folder = os.path.abspath(import_folder)

    # Create the data folder if it doesn't exist
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    # Create the import folder if it doesn't exist
    if not os.path.exists(import_folder):
        os.makedirs(import_folder)

    # Read the XLSX file
    sheet = pd.read_excel(xlsx_file)

    # Load the json config
    with open(config_file, 'r') as file:
        config = json.load(file)

    # Process entities and relations
    node_files = process_entities(sheet, config['entities'], import_folder)
    relationship_files = process_relations(sheet, config['relations'], import_folder)

    # Generate the Neo4j import command
    neo4j_command = (
            f'docker run --rm --publish=7474:7474 --publish=7687:7687 '
            f'--volume="{data_folder}":/data --volume="{import_folder}":/var/lib/neo4j/import neo4j '
            f'neo4j-admin database import full --report-file=import/import.report --overwrite-destination '
            + ' '.join([f'--nodes="import/{os.path.basename(node_file)}"' for node_file in node_files])
            + ' '
            + ' '.join([f'--relationships="import/{os.path.basename(relationship_file)}"' for relationship_file in relationship_files])
            + ' --skip-duplicate-nodes=true --skip-bad-relationships=true --verbose'
    )

    # Generate the Docker run command for after import
    docker_command = (
        f'docker run --rm --publish=7474:7474 --publish=7687:7687 '
        f'--volume="{data_folder}":/data --volume="{import_folder}":/var/lib/neo4j/import neo4j'
    )

    # Write the commands to importcmd.sh
    with open('importcmd.sh', 'w') as f:
        # Start with removing the data folder
        f.write(f'rm -rf {data_folder}\n')
        f.write(f'{neo4j_command}\n')
        f.write(f'{docker_command}\n')

    print("importcmd.sh file generated successfully.")

if __name__ == '__main__':
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process an XLSX file based on a config.json.')
    parser.add_argument('xlsx_file', type=str, help='The path to the input XLSX file.')
    parser.add_argument('config_file', type=str, help='The path to the config.json file.')
    parser.add_argument('data_folder', type=str, help='The relative path to the folder where CSV files will be generated.')
    parser.add_argument('import_folder', type=str, help='The relative path to the import folder for Neo4j.')

    # Parse the arguments
    args = parser.parse_args()

    # Run the processing function with the provided arguments
    process_xlsx_file(args.xlsx_file, args.config_file, args.data_folder, args.import_folder)
