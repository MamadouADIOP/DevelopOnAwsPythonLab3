import boto3, botocore, json, decimal, configparser
from boto3.dynamodb.conditions import Key, Attr
from boto3.dynamodb.types import TypeDeserializer


def main(ddbClient):
    
    ##load configuration data from file
    config = readConfig()

    tableName = config['tableName']
    pageSize = config['pageSize']

    print("\n************\nScanning with pagination...\n")
    queryAllNotesPaginator(ddbClient, tableName, pageSize)

def queryAllNotesPaginator(ddbClient, tableName, pageSize):

    ## TODO 6: Add code that creates a paginator and uses the printNotes function 
    # to print the items returned in each page.
    # response=ddbClient.scan(TableName=tableName,
    # Limit=int(pageSize))
    # print(response)
    paginator = ddbClient.get_paginator('scan')
    notes=[]
    page_iterator = paginator.paginate(TableName=tableName, Limit=int(pageSize))
    # response = [i for i in page_iterator]
    for page in page_iterator:
        notes.append(page["Items"])
    # print(response)
    
    notes= [n for n in notes if len(n) != 0]
    printNotes(notes[0])
    ## End TODO 6

## Utility methods
def printNotes(notes):
    if isinstance(notes, list):
        for note in notes:
            print(
                json.dumps(
                    {key: TypeDeserializer().deserialize(value) for key, value in note.items()},
                    cls=DecimalEncoder
                )
            )

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        if isinstance(o, set):  # <---resolving sets as lists
            return list(o)
        return super(DecimalEncoder, self).default(o)

def readConfig():
    config = configparser.ConfigParser()
    config.read('./labRepo/config.ini')

    return config['DynamoDB']

client = boto3.client('dynamodb')

try:
    main(client)
except botocore.exceptions.ClientError as err:
    print(err.response['Error']['Message'])
except botocore.exceptions.ParamValidationError as error:
    print(error)
