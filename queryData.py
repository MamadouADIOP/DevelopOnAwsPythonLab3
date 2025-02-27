import boto3, botocore, json, decimal, configparser
from boto3.dynamodb.conditions import Key, Attr
from boto3.dynamodb.types import TypeDeserializer


def main(ddbClient):
    
    ##load configuration data from file
    config = readConfig()

    tableName = config['tableName']
    UserId = config['queryUserId']

    print("\n************\nQuerying for notes that belong to user " + UserId + "...\n")
    printNotes(queryNotesByPartitionKey(ddbClient, tableName, UserId))

def queryNotesByPartitionKey(ddbClient, tableName, qUserId):
    ## TODO 5: Add code to query for a specific not with the parameter 
    # values available for use.
    response = ddbClient.query(
        TableName=tableName,
        ProjectionExpression='NoteId, Note',
        ExpressionAttributeValues={
            ':v1':{
                'S':qUserId}},
        KeyConditionExpression='UserId=:v1')

    
    ## End TODO 5
    return response["Items"]

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
