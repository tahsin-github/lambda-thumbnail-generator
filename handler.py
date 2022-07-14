from datetime import datetime
from io import BytesIO
from PIL import Image, ImageOps

import os
import uuid

from sqlalchemy import null
import boto3
import json

s3_client = boto3.client('s3')
image_size = int(os.environ['THUMBNAIL_SIZE'])


def thumbnail_generator(event, context):
    #parse event
    print("EVENT:::", event)
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key'] # The s3 link for the image
    image_size = event['Records'][0]['s3']['object']['size']


    if(not key.endsWith("_thumbnail.png")):
        image = get_s3_image(bucket_name, key)

        thumbnail = image_to_thumbnail(image)

        thumbnail_key = new_filename(key)

        url = upload_to_s3(key, bucket_name, thumbnail_key, thumbnail, image_size)

        return url

    body = {
        "message": "Go Serverless v3.0! Your function executed successfully!",
        "input": event,
    }

    return {"statusCode": 200, "body": json.dumps(body)}


def get_s3_image(bucket, key):
    """
    Returns the image file from S3 bucket.

    Keyword arguments:
    bucket -- the bucket name where the image is located
    key -- the s3 link to the image file
    """
    response = s3_client.get_object(Bucket = bucket, Key = key)
    image_content = response["Body"].read()

    file = BytesIO(image_content)
    image = Image.open(file)

    return image


def image_to_thumbnail(image):
    '''
    Resizes the image.
    '''
    return ImageOps.fit(image, (size, size), Image.ANTIALIAS)

def new_filename(Key):
    '''
    Creates a new file name.
    '''
    Key_split = Key.rsplit('.', 1)

    return Key_split[0] + "_thumbnail.png"


def upload_to_s3(bucket, key, image, img_size):
    '''
    Uploads file to the s3
    '''
    out_thumbnail = BytesIO() 


    image.save(out_thumbnail, 'PNG')
    out_thumbnail.seek(0)

    response = s3_client.put_object(
        ACL='public-read',
        Body=out_thumbnail,
        Bucket=bucket,
        ContentType='image/png',
        Key=key
    )
    print(response)

    url = '{}/{}/{}'.format(s3.meta.endpoint_url, bucket, key)



    return url
