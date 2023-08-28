import pandas as pd
import os
import json
import re
from jsonschema import validate

from google.oauth2 import service_account
from googleapiclient.discovery import build

def pull_sheet(spreadsheetId='18OKlXeiewO5W-dWsDas6wbbykBTWqyiJDVoqbVHIrt8'):
    """
    :param spreadsheetId: Google spreadsheet ID
    :return: a pandas dataframe created from the sheet
    """
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    SERVICE_ACCOUNT_FILE = '/Users/apiiro/Desktop/Files/keyToSheets/apiiro-rnd-9b9f6d10adf6.json'
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    spreadsheet = service.spreadsheets().values().get(spreadsheetId=spreadsheetId, range='A:Z').execute()
    rows = spreadsheet.get('values', [])
    df = pd.DataFrame(data=rows[1:], columns=rows[0])
    return df

def pull_schema():
    schema = {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "insightType": {
          "type": "string",
          "enum": [
            "ServiceDef",
            "APIHandler",
            "APICall",
            "MessageQueue"
          ]
        },
        "references": {
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "values": {
          "$ref": "#/definitions/DetailsSchema"
        }
      },
      "required": ["insightType", "references", "values"],
      "additionalProperties": False,
      "definitions": {
        "DetailsSchema": {
          "oneOf": [
            { "$ref": "#/definitions/ServiceDefDetailsSchema" },
            { "$ref": "#/definitions/APIHandlerDetailsSchema" },
            { "$ref": "#/definitions/APICallDetailsSchema" },
            { "$ref": "#/definitions/MessageQueueDetailsSchema" }
          ]
        },
        "ServiceDefDetailsSchema": {
          "type": "object",
          "properties": {
            "serviceName": { "type": "string" },
            "sourceFiles": {
              "type": "array",
              "items": { "type": "string" }
            },
            "installedPackages": {
              "type": "object",
              "additionalProperties": {
                "type": "string"
              }
            },
            "builtBinaries": {
              "type": "array",
              "items": { "type": "string" }
            },
            "runCommands": { "type": "string" },
            "externalConfigurations": {
              "type": "object",
              "additionalProperties": {
                "type": "string"
              }
            },
            "environmentVariables": {
              "type": "object",
              "additionalProperties": {
                "type": "string"
              }
            },
            "ports": {
              "type": "array",
              "items": {
                "type": "array",
                "items": { "type": "string" }
              }
            },
            "resourceRequirements": {
              "type": "object",
              "properties": {
                "cpu": { "type": "string" },
                "memory": { "type": "string" },
                "storage": { "type": "string" }
              }
            }
          },
          "required": ["serviceName", "sourceFiles", "installedPackages", "runCommands", "ports", "resourceRequirements"]
        },
        "APIHandlerDetailsSchema": {
          "type": "object",
          "properties": {
            "handlerSignature": { "type": "string" },
            "routes": {
              "type": "array",
              "items": { "type": "string" }
            },
            "httpMethods": {
              "type": "array",
              "items": { "type": "string" }
            }
          },
          "required": ["handlerSignature", "routes", "httpMethods"]
        },
        "APICallDetailsSchema": {
          "type": "object",
          "properties": {
            "apiEndpoint": { "type": "string" },
            "requestMethod": { "type": "string" },
            "parameters": {
              "type": "array",
              "items": { "type": "string" }
            },
            "headers": { "type": "object" }
          },
          "required": ["apiEndpoint", "requestMethod"]
        },
        "MessageQueueDetailsSchema": {
          "type": "object",
          "properties": {
            "queueName": { "type": "string" },
            "messageType": { "type": "string" },
            "publishingService": { "type": "string" },
            "consumingService": { "type": "string" },
            "messagePayloadSchema": { "type": "object" }
          },
          "required": ["queueName", "messageType", "publishingService", "consumingService", "messagePayloadSchema"]
        }
      }
    }

    return schema

def convert_port_to_list(string):
    if string is None:
        return []
    inner_lists = string.strip('[]').split('],[')
    # Splitting each inner list into individual elements and converting to string
    result = [[str(element) for element in sublist.split(',')] for sublist in inner_lists]
    if result == [[""]]:
        return []
    return result

def convert_packages_to_dict(string):
    result = json.loads(string) if string else {}
    return result

def convert_paths_to_list(string):
    if string is None:
        return []
    inner_list = string.strip('[]').split(',')
    result = [str(element) for element in inner_list]
    if result == [""]:
        return []
    return result

def fit_df_to_schema(df):

    for index, row in df.iterrows():
        try:
            row['ports'] = convert_port_to_list(row['ports'])
        except Exception as e:
            print(f"ports error - row {index} in the file: {row['ports']}.")
        try:
            row['installedPackages'] = convert_packages_to_dict(row['installedPackages'])
        except Exception as e:
            print(f"installed_packages error - row {index} in the file: {row['installedPackages']}.")
        try:
            row['resourceRequirements'] = convert_packages_to_dict(row['resourceRequirements'])
        except Exception as e:
            print(f"resourceRequirements error - row {index} in the file: {row['resourceRequirements']}.")
        try:
            row['references'] = convert_paths_to_list(row['references'])
        except Exception as e:
            print(f"references error - row {index} in the file: {row['references']}.")
        try:
            row['sourceFiles'] = convert_paths_to_list(row['sourceFiles'])
        except Exception as e:
            print(f"sourceFiles error - row {index} in the file: {row['sourceFiles']}.")
        try:
            row['builtBinaries'] = convert_paths_to_list(row['builtBinaries'])
        except Exception as e:
            print(f"builtBinaries error - row {index} in the file: {row['builtBinaries']}.")

    return df

def validate_json(json_data):
    '''
    Validate a single json with a schema provided by pull_schema.
    :param json_data: A json line of one instance to be validated
    :return: error if it doesn't fit the schema provided
    '''
    schema = pull_schema()
    return validate(instance=json_data, schema=schema)

def covert_row_to_json(row):
    json_data = {
        "insightType": str(row["insightType"]),
        "references": row["references"],
        "values": {
            "serviceName": str(row["serviceName"]),
            "sourceFiles": row["sourceFiles"],
            "builtBinaries": row["builtBinaries"],
            "installedPackages": row["installedPackages"],
            "ports": row["ports"],
            "runCommands": str(row["runCommands"]),
            "resourceRequirements": row["resourceRequirements"],
            "description": str(row["description"])
        }
    }
    return json_data

def df_to_jsons(dest_path,spreadsheetId):
    '''
    :param dest_path: a destination path for the folders containing the jsons
    :param spreadsheetId: spreadsheetID of Google sheets
    for each repo in the code_base a json file is created according to the scheme.
    Please notice dest_path is relative to where the function is called from, I think
    Sends each code_base to code_base_to_jsons
    '''
    df = pull_sheet(spreadsheetId) # create a df from the sheet
    code_base_names = df['codeBase'].unique() # extract code bases names to code_bases
    for code_base in code_base_names:
        df_code_base = df[df['codeBase'] == code_base] # extract a single code base from the df
        code_base_path = os.path.join(dest_path, df_code_base['codeBase'].iloc[0])
        if not os.path.exists(code_base_path):
            os.makedirs(code_base_path)  # Create a folder with the df_code_base name in the destination folder
        code_base_to_jsons(df_code_base.drop(columns=['codeBase']), code_base_path) # send it to a function that creates jsons from the codebase

def code_base_to_jsons(df, dest_path):
    """
    :param df: a code_base pandas dataframe without a column stating the code_base, only repos
    :param dest_path: the code_base destination path, where each repo will be represented in a folder
    sends each repo to repos_to_jsons
    """
    repo_names = df['repo'].unique()
    for repo in repo_names:
        df_repo = df[df['repo'] == repo] # extract a single repo from the code_base
        repo_to_jsons(repo, df_repo.drop(columns=['repo']),dest_path)

def repo_to_jsons(repo_name, df, dest_path):
    '''
    Create a json from df row. validate json with schema. Write it to a file in a specified path.
    Write the json lines into the repo's json path provided, after validating the json created with a provided schema.
    :param repo_name: Repo name
    :param df: Dataframe containing data about a single repo of a single code-base
    :param dest_path: the destination path of the json file of the repo
    '''
    df = fit_df_to_schema(df) # Fit the data frame to the schema - exchange strings with arrays and objects, etc.
    json_file_path = os.path.join(dest_path, f"{repo_name}.json")
    with open(json_file_path, "a") as json_file:
        for index, row in df.iterrows(): # for each row (each instance) create a json line, and upload it to the file
            json_data = covert_row_to_json(row)
            try:
                validate_json(json_data)
            except Exception as e:
                print( f"json line {index} invalid: \n{e}")
            json_file.write(json.dumps(json_data) + '\n')

df_to_jsons('jsons','18OKlXeiewO5W-dWsDas6wbbykBTWqyiJDVoqbVHIrt8')