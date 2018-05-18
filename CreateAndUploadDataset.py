# Imports
import csv
import json
import requests

# Set up variables
file_to_load = 'COMPTHER.csv'
path_to_load = '/Users/paulcourtney/Google Drive/Pharma Demo/COMPTHER.csv'
id_field = 'record_id'

# Set id_generated to True if there is no id field in the CSV file; this is the default
id_generated = True

delimiter = ','
description = 'Sample file ingested by public API'
base_url = 'http://10.20.0.76:9100'
auth = 'BasicCreds YWRtaW46ZHQ='

# Create Dataset without any columns or rows using the Public API 'pubapi/v1/datasets'
def createDataset(datasetName, keyAttributeNames, description=""):
    resp = requests.post(base_url + '/api/pubapi/v1/datasets'
				, headers={'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': auth}
				, data=json.dumps({"name":datasetName, 
                                    "description":description, 
                                    "keyAttributeNames":keyAttributeNames}))
    print('Creating dataset')
    print('Status code: '+ str(resp.status_code)) 
    # get the id for the new dataset (number at the end of URI)
    full_dataset_id = resp.json()['id']
    dataset_id = full_dataset_id.split('/')[-1] 
    return dataset_id

# Define the columns, or attributes, for the dataset
def addAttributes(datasetId, attributeName):
        resp = requests.post(base_url + '/api/pubapi/v1/datasets/{0}/attributes'.format(datasetId)
				, headers={'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': auth}
				, data=json.dumps({"name": attributeName,
								   "type": {"baseType": "STRING"}
								  }))
        print('Adding attribute')
        print('Status code: '+ str(resp.status_code))  

def updateRecords(datasetId, json_filename):
    with open(json_filename) as json_file:
        resp = requests.post(base_url + '/api/pubapi/v1/datasets/{0}:updateRecords?header=false'.format(datasetId)
				, headers={'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': auth}
				, data=json_file.read())
        print('Updating records')
        print('Status code: '+ str(resp.status_code))
        
# Step 1: create the dataset in Unify and keep the id for it
dataset_id = createDataset(file_to_load, [id_field], description)
print('Created dataset with id '+ dataset_id)

# Step 2: create the attributes
with open(path_to_load, 'r') as csvfile:
    csv_data = csv.DictReader(csvfile, delimiter=delimiter)
    columns = next(csv_data, None)
    for c in columns:
    	print('Create attribute for column '+ c)
    	addAttributes(dataset_id, c)
        
# Step 3: keep reading the csv file to make json for "create" actions and call update records; if the id is to be 
# generated, use record_id and increment for each row. Otherwise, use the id included in the csv file. 
    json_filename = file_to_load + '.json'
    with open(json_filename, 'w') as outfile:
        record_id = 1
        for record in csv_data:
            if id_generated: 
                create_entry = {'action': 'CREATE', 'recordId': record_id, 'record': record}
                record_id = record_id + 1
            else: 
                create_entry = {'action': 'CREATE', 'recordId': record[id_field], 'record': record}
            outfile.write(json.dumps(create_entry)+'\n')  
    updateRecords(dataset_id, json_filename)
