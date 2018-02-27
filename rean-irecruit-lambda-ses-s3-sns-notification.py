from __future__ import print_function

import os
import json
import boto3

sns = boto3.client('sns')


def lambda_handler(event, context):
    
    data = event
    message = data['Records'][0]['Sns']['Message']
    result= '\n' + '* * * * * * * * * *\n\n'
    
    try:
        parsed_json = json.loads(message)
    
        nameList = ['From', 'Date', #list of needed headers
                    'Subject', 'To','Reply-to','Subject', 'Cc', 'Content-Type', 'X-Original-Sender']
        
        for x in range(0,len(nameList)): #for loop to account for lower case matches
            nameList[x] = nameList[x].lower()
            
        for header in parsed_json['mail']['headers']: #for loop to print out each of the headers name and value
            if header['name'].lower() in nameList:
                result += header['name'] + ': ' +  header['value'] + '\n'
                
        forLength = len(parsed_json['receipt']['recipients'])
        result += 'Recipients: '
        for x in range(0, forLength):#for loop to print out list of items
            if (x + 1) is not forLength:
                result += str(parsed_json['receipt']['recipients'][x]) + ', '
            else:
                result += str(parsed_json['receipt']['recipients'][x]) + '\n'
        
        result += 'Action.Type: ' + parsed_json['receipt']['action']['type'] + '\n'
        result += 'Action.BucketName: ' + parsed_json['receipt']['action']['bucketName'] + '\n'
        result += 'Action.ObjectKey: ' + parsed_json['receipt']['action']['objectKey'] + '\n'
    except:
        result += str(message) + '\n'
        
    result += '\n' + '* * * * * * * * * *'
    
    response = sns.publish(
        TopicArn="arn:aws:sns:us-east-1:256555058276:rean-irecruit-ses-to-s3-info",
        Message=result, 
        Subject='Amazon SES Email to S3 Information')
    return print(result)