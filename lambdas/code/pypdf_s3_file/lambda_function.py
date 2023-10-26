import PyPDF2 
import boto3  
import os  
import json



def lambda_handler(event, context):
    print (event)
    s3_uri = event['s3_uri']
    local_path = "/tmp"
    local_file = download_s3_file(s3_uri,local_path)
    text = extract_text(local_file)
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
    s3 = boto3.client('s3')  

    # Download file from S3 to local folder  
    s3.download_file(bucket, key, f"{local_path}/{filename}")
    return f"{local_path}/{filename}"


def extract_text(file):
    with open(file, 'rb') as pdfFileObj:
        pdfReader = PyPDF2.PdfReader(pdfFileObj) 
        count = 0
        text = []
        for pageObj in pdfReader.pages:
            count +=1
            text.append(pageObj.extract_text())

    return {"num_pages":count, "pages": text}


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


