import csv
import json

# Load the existing JSON template
json_template = {
    "MeasurementToolMetadata": {
        "Description": "BrainSwipes",
        "TermURL": "https://www.brainswipes.us/",
        "Name": "BrainSwipes Results for Quality Control Performed on XCP-D T2w Structural Derivatives",
        "Category": [
            "QC",
            "sMRI"
        ]
    }
}
# Default structure for KEYNAME
default_key_structure = {
    "name": "n/a",
    "Description": "n/a",
    "instruction": "n/a",
    "header": "n/a",
    "Units": "n/a",
    "note": "n/a",
    "table_name": "img_brainswipes_xcpd-T2w",
    "table_label": "T2w",
    "order_display": "n/a",
    "domain": "Tabular Imaging",
    "study": "Substudy",
    "type_field": "n/a",
    "type_level": "interval",
    "type_var": "n/a",
    "type_data": "n/a",
    "source": "Child",
    "collection_platform": "n/a",
    "loris_required": "n/a",
    "branching_logic": "n/a",
    "url_table": "n/a",
    "url_warn_use": "n/a",
    "url_warn_data": "n/a",
    "url_table_warn_use": "n/a",
    "url_table_warn_data": "n/a",
    "identifier_columns": "participant_id | session_id | run_id",
    "order_sort": "n/a"
}

# Read the CSV file and update the JSON
def update_json_from_csv(csv_file, json_template):
    with open(csv_file, mode='r', newline='') as file:
        reader = csv.DictReader(file)
        for row in reader:
            keyname = row.pop('KEYNAME')  # Extract the KEYNAME
            
            # Initialize with default structure if not present
            json_template[keyname] = json_template.get(keyname, default_key_structure.copy())
            
            for key, value in row.items():
                if value != 'n/a':  # Replace only if the value is not 'n/a'
                    json_template[keyname][key] = value

    return json_template

csv_file = 'files/T2w.csv'

# update json and save
generated_json = update_json_from_csv(csv_file, json_template)

with open('updated_data.json', 'w') as json_file:
    json.dump(generated_json, json_file, indent=4)

