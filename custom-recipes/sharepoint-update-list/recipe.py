import dataiku
import pandas as pd
from dataiku.customrecipe import get_input_names_for_role, get_recipe_config, get_output_names_for_role
from safe_logger import SafeLogger
from dss_constants import DSSConstants
from sharepoint_client import SharePointClient
import json

logger = SafeLogger("sharepoint-online plugin", DSSConstants.SECRET_PARAMETERS_KEYS)
logger.info('SharePoint Online append to list recipe v{}'.format(DSSConstants.PLUGIN_VERSION))

def convert_date_int_format(json_row):
    # Convert pandas timestamps to iso
    for key in json_row:
        value = json_row.get(key)
        if pd.isna(value):
            json_row[key] = ""
        elif type(value) == pd.Timestamp:
            json_row[key] = str(value.strftime(DSSConstants.DATE_FORMAT))
        elif type(value) == int:
            json_row[key] = str(json_row[key])
    return json_row

# Retrieve input_dataset and input_schema
input_dataset_names = get_input_names_for_role('input_dataset')
input_dataset = dataiku.Dataset(input_dataset_names[0])
input_dataframe = input_dataset.get_dataframe()
input_schema = input_dataset.read_schema()

# Retreive output_dataset and retrieve schema from input dataset
# Update schema to include Sharepoint REST API Result

output_dataset_names = get_output_names_for_role('api_output')
output_dataset = dataiku.Dataset(output_dataset_names[0])
input_schema.append({'name': "Sharepoint_Result", 'type': 'string'})
output_dataset.write_schema(input_schema)

# Retrieve Recipe Configuration
config = get_recipe_config()
dku_flow_variables = dataiku.get_flow_variables()

# Set Sharepoint List Title and Sharepoint Auth Type
sharepoint_list_title = config.get('sharepoint_list_title')
auth_type = config.get('auth_type')
logger.info('init:sharepoint_list_title={}, auth_type={}'.format(sharepoint_list_title, auth_type))

# Set Sharepoint Tenant and Sharepoint Site, based on User Config
advanced_parameters = config.get('advanced_parameters')
sharepoint_tenant = config.get('sharepoint_oauth').get('sharepoint_tenant')
sharepoint_site = config.get('sharepoint_oauth').get('sharepoint_site')
sharepoint_site_overwrite = config.get('sharepoint_site_overwrite', "")

if advanced_parameters & (sharepoint_site_overwrite != ""):
    current_site = sharepoint_site_overwrite
else:
    current_site = sharepoint_site
logger.info('init:sharepoint_site={}'.format(current_site))
    
# Set Base Sharepoint URL for REST API calls
base_url = sharepoint_tenant + '/' + current_site + '/' + "_api"
list_base_url = base_url + '/web/lists'

# Establish Sharepoint Client and Session
client = SharePointClient(config)

# Collect from current session the FormDigestValue and store as "thetoken"
thecontext = client.session.post(f"{base_url}/contextinfo")
thedict = json.loads(thecontext.text)
thetoken = thedict['FormDigestValue']

# Create a two way mapping of Sharepoint Fields: StaticName <-> Title
# new_column_names is a dictionary which maps Titles -> StaticName
# restore_column_names is a dictionary which maps StaticName -> Titles

fields = client.session.get(f"{list_base_url}/getbytitle('{sharepoint_list_title}')/fields")

new_column_names = {}
restore_column_names = {}
for item in fields.json()['d']['results']:
    
    # Do not need to retrieve Computed, Lookup, or File Fields
    if (item['TypeAsString'] == 'Computed') | (item['TypeAsString'] == 'Lookup') | (item['TypeAsString'] == 'File'):
        pass
    else:
        new_column_names[item['Title']] = item['StaticName']
        restore_column_names[item['StaticName']] = item['Title']

# Rename the Title column names to StaticName column names for REST API calls, also set index to ID
try:
    input_dataframe = input_dataframe.rename(columns=new_column_names).set_index(['ID'])
except KeyError:
    raise KeyError("Error: Input Dataset must include a column named ID.")
    logging.error("Your Input Dataset must include a column named ID.")

with output_dataset.get_writer() as writer:
    for index, input_parameters_row in input_dataframe.iterrows():
        
        # For storing data to be written to output_dataset
        write_data = {}
        
        # For sending data to the REST API for Get and Post
        json_row = input_parameters_row.to_dict()
        json_row = convert_date_int_format(json_row)
        print(f"Trying to update ID: {index}")
        
        # GET REST API call to retrieve the metadata "type" and "etag" (required to POST)
        orig = client.session.get(f"{list_base_url}/getbytitle('{sharepoint_list_title}')/items({index})")
        print(f"Result to verify ID exists: {orig}")
        
        if str(orig) != '<Response [200]>':
            logging.warning(f"Warning: Unfortunately, no matching ID {index} was found to update. Result: {orig}")
            continue
        
        list_type = orig.json()['d']['__metadata']['type']
        etag = orig.json()['d']['__metadata']['etag']
        
        # Create JSON for sending to the POST REST API
        data = {"__metadata": {"type": list_type}}
        for key in json_row:
            
            # By default, if a value is blank in input_dataset, it will not update value in Sharepoint List
            if json_row[key] == "":
                pass
            
            # Adds updated data to the JSON for sending to POST REST API
            else:
                data[key] = json_row[key]
            
            # Adds updated data to be written to output_dataset, restoring column names from StaticName -> Title
            write_data[restore_column_names[key]] = json_row[key]
            
        # Create Header for POST REST API, using thetoken and etag
        headers = {'X-RequestDigest': thetoken,
            'Content-Length': str(len(data)),
            'Accept': 'application/json;odata=verbose',
            'Content-Type': 'application/json;odata=verbose',
            'X-HTTP-METHOD': 'MERGE',
            'IF-MATCH': etag}
        
        # POST REST API Call, writing updated data to each item with ID == index (from input_dataset)
        new = client.session.post(f"{list_base_url}/GetByTitle('{sharepoint_list_title}')/items({index})",
                                  json=data,
                                  headers=headers)
        if str(new) != '<Response [204]>':
            logging.warning(f"Warning: Unfortunately, ID {index} was not properly updated. Result: {new}")
            continue
        
        logging.info(f"ID {index} was successfully updated! Result: {new}")
        
        # Add metadata to output_dataset
        write_data['ID'] = index
        write_data["Sharepoint_Result"] = 'Success: ' + str(new)
        writer.write_row_dict(write_data)
        

