import json
import base64
import boto3
import logging

s3 = boto3.client('s3', endpoint_url='https://s3.ap-south-1.amazonaws.com', region_name='ap-south-1')
bucket_name = "deepen-poc-s3"
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    if event["httpMethod"] == "POST":
        return handle_post(event)
    elif event["httpMethod"] == "GET":
        return handle_get(event)
    elif event["httpMethod"] == "DELETE":
        return handle_delete(event)
    else:
        return {
            'statusCode': 400,
            'body': json.dumps('Error: Unsupported HTTP method or path.')
        }

def handle_post(event):
    try:
        body = json.loads(event['body'])

        if "file" not in body:
            return {
                'statusCode': 400,
                'body': json.dumps('Error: File parameter is missing.')
            }

        filename = body["file"]
        logger.info(f"File name: {filename}")

        if "content" in body:
            get_file_content = body["content"]
            decode_content = base64.b64decode(get_file_content)
            s3_upload = s3.put_object(Bucket=bucket_name, Key=filename, Body=decode_content)
            s3_url = f"https://{bucket_name}.s3.ap-south-1.amazonaws.com/{filename}"

            response_data = {
                'statusCode': 201,
                's3_url': s3_url
            }
            return {
                'statusCode': 201,
                'body': json.dumps({'data': response_data})
            }

        else:
            presigned_url = generate_presigned_url(filename)

            response_data = {
                'file-url': presigned_url
            }

            return {
                'statusCode': 201,
                'body': json.dumps({'data': response_data})
            }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def generate_presigned_url(file_name):
    try:
        presigned_url = s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': bucket_name, 'Key': file_name},
            ExpiresIn=300,
        )
        return presigned_url
    except Exception as e:
        raise e

def handle_get(event):
    try:
        response = s3.list_objects(Bucket=bucket_name)
        details = response.get('Contents', [])
        object_keys = [obj['Key'] for obj in details]

        return {
            'statusCode': 201,
            'body': json.dumps({"Data": object_keys})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def handle_delete(event):
    logger.info(f"Received event: {event}")

    try:
        body = json.loads(event['body'])

        if "file" not in body:
            return {
                'statusCode': 400,
                'body': json.dumps('Error: File parameter is missing in the request body.')
            }

        filename = body["file"]
        logger.info(f"File name: {filename}")

        s3_delete = s3.delete_object(Bucket=bucket_name, Key=filename)

        return {
            'statusCode': 201,
            'body': json.dumps(f'The file {filename} is deleted successfully!')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def handle_presigned_url(event):
    logger.info(f"Received event: {event}")

    try:
        body = json.loads(event['body'])

        if "file" not in body:
            return {
                'statusCode': 400,
                'body': json.dumps('Error: File parameter is missing.')
            }

        filename = body["file"]
        logger.info(f"File name: {filename}")

        presigned_url = generate_presigned_url(filename)

        response_data = {
            'file-url': presigned_url
        }

        return {
            'statusCode': 201,
            'body': json.dumps({'data': response_data})
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
