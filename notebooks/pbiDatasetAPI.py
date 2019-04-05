# Databricks notebook source
# DBTITLE 1,pbiDatasetAPI Class
import requests
import json
import datetime
from pyspark.sql.functions import to_json, struct


class pbiDatasetAPI:
  
  def __init__(self, username, password, application_id):
    """Initialise the calss instance"""
    self.username = username
    self.password = password
    self.application_id = application_id
    self.getHeaders()
    
    
  def getToken(self, tokenStringOnly = True):
    """Acquire an access token"""
    data = {
        "grant_type": "password"
      , "scope"     : "openid"
      , "resource"  : "https://analysis.windows.net/powerbi/api"
      , "client_id" : self.application_id
      , "username"  : self.username
      , "password"  : self.password
    }
    token = requests.post("https://login.microsoftonline.com/common/oauth2/token", data = data)
    assert token.status_code == 200, "Failed to retrieve token: {}".format(token.text)
    self.token = token
    self.accessToken = token.json()['access_token']
    self.tokenExpirationDateTime = datetime.datetime.fromtimestamp(int(token.json()["expires_on"]))
    if tokenStringOnly:
      return self.accessToken
    else:
      return self.token


  def getHeaders(self):
    """Create HTTP headers"""
    self.headers = {
        #"Content-Type": "application/json; charset=utf-8",
        "Authorization": "Bearer {}".format(self.getToken())
    }
    return self.headers


  def executePBIRequest(self, endpoint, body = None, method = "get"):
    """Execute HTTP request against the Power BI REST API"""
    method = method.lower()
    assert method in ("get", "post", "put", "delete"), "Invalid method: {}".format(method)
    api_endpoint = "https://api.powerbi.com/v1.0/myorg/" + endpoint
    if method == "get":
      if body == None:
        response = requests.get(api_endpoint, headers = self.headers)
      else:
        response = requests.get(api_endpoint, headers = self.headers, json = body)
    elif method == "post":
      if body == None:
        response = requests.post(api_endpoint, headers = self.headers)
      else:
        response = requests.post(api_endpoint, headers = self.headers, json = body)
    elif method == "put":
      response = requests.put(api_endpoint, headers = self.headers, json = body)
    elif method == "delete":
      response = requests.delete(api_endpoint, headers = self.headers)
    return response


  def getPBIDataType(self, sparkType):
    """Convert a spark data type to an Entity Data Model data type"""
    sparkType = sparkType.lower()
    if   sparkType == "byte":
      return "Byte"
    elif sparkType in ("shortint", "short"):
      return "Int16"
    elif sparkType in ("int", "integer"):
      return "Int32"
    elif sparkType in ("bigint", "long"):
      return "Int64"
    elif sparkType == "float":
      return "Float"
    elif sparkType == "double":
      return "Double"
    elif sparkType[:7] == "decimal":
      return "Decimal" + sparkType[7:]
    elif sparkType == "string":
      return "String"
    elif sparkType == "binary":
      return "Binary"
    elif sparkType == "boolean":
      return "Boolean"
    elif sparkType == "timestamp":
      return "DateTime"
    elif sparkType == "date":
      return "DateTime"
    return None


  def getPBITableColumns(self, dataFrame):
    """Create a column list (with properties) in JSON-format from a DataFrame"""
    schemaFields = dataFrame.schema.jsonValue()["fields"]
    result = ""
    for item in schemaFields:
      result += (
          '{' 
        + '    "name": "{}"'.format(item["name"])
        + '  , "dataType": "{}"'.format(self.getPBIDataType(item["type"]))
        + '},\r\n'
      )
    result = result[:-3]  # Remove the trailing comma and new line
    return result


  def getPBITableRows(self, dataFrame):
    """Create data rows in JSON-format from a DataFrame"""
    result = ""
    rows = dataFrame.select(to_json(struct([dataFrame[x] for x in dataFrame.columns])).alias("value")).collect()
    for row in rows:
      result = result + row.value + ",\r\n"
    result = result[:-3]  # Remove the trailing comma and new line
    return result


  def executePBIOperation(  self
                          , operation
                          , groupId = None
                          , datasetKey = None
                          , datasetName = None
                          , tableNames = None
                          , dataFrames = None
                          , reCreateIfExists = False
                         ):
    """Execute a Power BI API operation"""
    
    operation = operation.lower()

    # If groupId parameter is provided the request URL will be appropriately adjusted
    groupPrefix = "" if groupId is None else "groups/{}/".format(groupId)
    
    # If datasetName parameter is provided create a list of all dataset keys
    # correpsonding to datasets with that name
    if datasetName is not None:
      self.datasetKeys = []
      operationURL = "{groupPrefix}datasets".format(groupPrefix = groupPrefix)
      allKeys = self.executePBIRequest(endpoint = operationURL, method = "get")
      allKeys = allKeys.json()["value"]
      allKeys = [obj for obj in allKeys if obj["name"] == datasetName]
      for datasetK in allKeys:
        self.datasetKeys.append(datasetK["id"])
    
    # Datasets - Delete Dataset By Id
    if   operation == "deletedatasetbyid":
      assert datasetKey is not None \
      , "Error: datasetKey parameter is mandatory for the {} operation.".format(operation)
      operationURL = "{groupPrefix}datasets/{datasetKey}".format(groupPrefix = groupPrefix, datasetKey = datasetKey)
      requestResult = self.executePBIRequest(endpoint = operationURL, method = "delete")

    # Custom: Datasets - Delete Dataset By Name
    elif operation == "deletedatasetbyname":
      assert datasetName is not None \
      , "Error: datasetName parameter is mandatory for the {} operation.".format(operation)
      requestResult = "Dataset '{}' not found. No operation performed.".format(datasetName)
      for datasetKey in self.datasetKeys:
        operationURL = "{groupPrefix}datasets/{datasetKey}".format(groupPrefix = groupPrefix, datasetKey = datasetKey)
        requestResult = self.executePBIRequest(endpoint = operationURL, method = "delete")

    # Datasets - Get Dataset By Id
    elif operation == "getdatasetbyid":
      assert datasetKey is not None \
      , "Error: datasetKey parameter is mandatory for the {} operation.".format(operation)
      operationURL = "{groupPrefix}datasets/{datasetKey}".format(groupPrefix = groupPrefix, datasetKey = datasetKey)
      requestResult = self.executePBIRequest(endpoint = operationURL, method = "get")

    # Custom: Datasets - Get Dataset By Name
    elif operation == "getdatasetbyname":
      assert datasetName is not None \
      , "Error: datasetName parameter is mandatory for the {} operation.".format(operation)
      operationURL = "{groupPrefix}datasets".format(groupPrefix = groupPrefix)
      requestResult = self.executePBIRequest(endpoint = operationURL, method = "get")
      requestResult = requestResult.json()["value"]
      requestResult = [obj for obj in requestResult if(obj["name"] == datasetName)]
      
    # Datasets - Get Datasets
    elif operation == "getdatasets":
      operationURL = "{groupPrefix}datasets".format(groupPrefix = groupPrefix)
      requestResult = self.executePBIRequest(endpoint = operationURL, method = "get")

    # Push Datasets - Datasets DeleteRows
    elif operation == "deleterows":
      assert ((datasetKey is not None)
          and (tableNames is not None)) \
      , "Error: datasetKey and tableNames parameters are mandatory for the {} operation.".format(operation)
      requestResult = []
      for tableName in tableNames:
        operationURL = "{groupPrefix}datasets/{datasetKey}/tables/{tableName}/rows".format(groupPrefix = groupPrefix, datasetKey = datasetKey, tableName = tableName)
        requestResult.append(self.executePBIRequest(endpoint = operationURL, method = "delete"))

    # Push Datasets - Datasets GetTables
    elif operation == "gettables":
      assert datasetKey is not None \
      , "Error: datasetKey parameter is mandatory for the {} operation.".format(operation)
      operationURL = "{groupPrefix}datasets/{datasetKey}/tables".format(groupPrefix = groupPrefix, datasetKey = datasetKey)
      requestResult = self.executePBIRequest(endpoint = operationURL, method = "get")

    # Push Datasets - Datasets PostDataset
    elif operation == "postdataset":
      assert ((datasetName is not None) 
          and (tableNames  is not None) 
          and (dataFrames  is not None)) \
        , "Error: datasetName, tableNames and dataFrames parameters are mandatory for the {} operation.".format(operation)
      datasets = self.executePBIOperation("getdatasetbyname", groupId = groupId, datasetName = datasetName)
      if not reCreateIfExists:
        assert len(datasets) == 0, "Error: dataset(s) with the name '{}' already exist(s).".format(datasetName)
      else:
        # Delete all existing datasets with the supplied name before creating it
        self.executePBIOperation("deletedatasetbyname", groupId = groupId, datasetName = datasetName)
      operationURL = "{groupPrefix}datasets".format(groupPrefix = groupPrefix)
      # Create a request body in JSON-format based on tableNames and dataFrames metadata
      body = '''
      {
        "name": "''' + datasetName + '''",
        "defaultMode": "Push",
        "tables": [
      '''
      for tableName, dataFrame in zip(tableNames, dataFrames):
        body += ('''
            {
              "name": "''' + tableName + '''",
              "columns": [
        ''' + self.getPBITableColumns(dataFrame) + '''
              ]
            },\r\n''')
      body = body[:-3]  # Remove the trailing comma and new line
      body += ('''
        ]
      }
      ''')
      requestResult = self.executePBIRequest(endpoint = operationURL, body = json.loads(body), method = "post")
      self.datasetKey = [requestResult.json()["id"]]

    # Push Datasets - Datasets PostRows
    elif operation == "postrows":
      assert ((datasetKey is not None) 
          and (tableNames is not None) 
          and (dataFrames is not None)) \
      , "Error: datasetKey, tableNames and dataFrames parameters are mandatory for the {} operation.".format(operation)
      requestResult = []
      for tableName, dataFrame in zip(tableNames, dataFrames):
        operationURL = "{groupPrefix}datasets/{datasetKey}/tables/{tableName}/rows".format(groupPrefix = groupPrefix, datasetKey = datasetKey, tableName = tableName)
        # Create a request body in JSON-format based on the data in dataFrames
        body = '''
        {
          "rows": [
        ''' + self.getPBITableRows(dataFrame) + '''
          ]
        }
        '''
        requestResult.append(self.executePBIRequest(endpoint = operationURL, body = json.loads(body), method = "post"))

    # Push Datasets - Datasets PutTable
    elif operation == "puttable":
      assert ((datasetKey is not None) 
          and (tableNames is not None) 
          and (dataFrames is not None)) \
        , "Error: datasetKey, tableNames and dataFrames parameters are mandatory for the {} operation.".format(operation)
      requestResult = []
      for tableName, dataFrame in zip(tableNames, dataFrames):
        operationURL = "{groupPrefix}datasets/{datasetKey}/tables/{tableName}".format(groupPrefix = groupPrefix, datasetKey = datasetKey, tableName = tableName)
        body = ('''
            {
              "name": "''' + tableName + '''",
              "columns": [
        ''' + self.getPBITableColumns(dataFrame) + '''
              ]
            }''')
        requestResult.append(self.executePBIRequest(endpoint = operationURL, body = json.loads(body), method = "put"))

    else:
      requestResult = None
      print("Invalid operation provided: '{}'".format(operation))

    return requestResult