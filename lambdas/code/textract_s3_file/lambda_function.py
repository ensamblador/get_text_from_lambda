
import boto3  
import os  
import json

textract = boto3.client('textract')
s3 = boto3.client('s3')


def lambda_handler(event, context):
    print (event)
    s3_uri = event['s3_uri']
    text = amazon_textract_less_5mb(s3_uri)
    upload_json_to_s3(text, f"{s3_uri}.json")
    event['transcript']= [f"{s3_uri}.json"]
    return event




def download_s3_file(s3_uri, local_path):  

    # Split the S3 URI to get bucket and key  
    s3_components = s3_uri.split('/')  
    bucket = s3_components[2]
    key = '/'.join(s3_components[3:])  
    filename = key.split("/")[-1]
    print(bucket, key)
    # Create S3 client  

    # Download file from S3 to local folder  
    s3.download_file(bucket, key, f"{local_path}/{filename}")
    return f"{local_path}/{filename}"



def upload_json_to_s3(data, s3_uri):  

  # Parse S3 URI to get bucket and key  
  s3_components = s3_uri.split('/')  
  bucket = s3_components[2]  
  key = '/'.join(s3_components[3:])  

  # Convert Python object to JSON string  
  json_data = json.dumps(data)  

  # Create S3 client  
  s3 = boto3.client('s3')  

  # Upload JSON string as a file to S3  
  s3.put_object(Body=json_data, Bucket=bucket, Key=key)


def amazon_textract_less_5mb(s3_uri):

    bucket = s3_uri.split('/')[2]
    key =  '/'.join(s3_uri.split('/')[3:])

    print(bucket, key)

    response = s3.head_object(Bucket=bucket, Key=key)
    size = response['ContentLength'] 

    if size > 5000000:
        print("Object is greater than 5MB")
    else:
        print("Object is less than or equal to 5MB")

    response = textract.detect_document_text(
        Document={
            'S3Object': {
                'Bucket': bucket,
                'Name': key
            }
        })
    n_pages = response['DocumentMetadata']['Pages']
    pages = {}

    text = ''
    for item in response['Blocks']:
        item_id = item['Id']
        if item['BlockType'] == 'PAGE':   
            relationships = item['Relationships']
            for relationship in relationships:
                if relationship['Type'] == 'CHILD':
                    pages[item_id] = {'CHILDS': relationship['Ids'], 'LINES': []}

        if item['BlockType'] == 'LINE':
            for page in pages.keys():
                if item_id in pages[page]['CHILDS']:
                    pages[page]['LINES'].append(item['Text'])


    return {"num_pages":n_pages, "pages": ['\n'.join(pages[key]['LINES']) for key in pages.keys()]}