from datetime import datetime
import sys
import json
import requests
import boto3

def lambda_handler(event, context):

    # enter your api key here
    api_key ='API_KEY'
    
    # List of origins Lat Longs
    source = '-17.633426%2C-149.60669|-17.630098%2C-149.607014'
    
    # List of destibnations Lat Longs
    dest = '-17.630098%2C-149.607014|-17.633426%2C-149.60669'
    
    # url variable store url
    url ='https://maps.googleapis.com/maps/api/distancematrix/json?'
    
    # Get method of requests module
    # return response object
    r = requests.get(url + 'origins=' + source + '&destinations=' + dest +'&key=' + api_key)
    
    # json method of response object
    # return json format result
    x = r.json()
    
    # by default driving mode considered
    
    # datetime object containing current date and time
    now = datetime.now()
    # dd/mm/YY H:M:S
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
    
    # Build csv file
    string = dt_string + ',' \
        + x['origin_addresses'][0] + ',' \
        + x['destination_addresses'][0] + ',' \
        + str(x['rows'][0]['elements'][0]['distance']['value']) + ',' \
        + str(x['rows'][0]['elements'][0]['duration']['value']) + '\n' \
        + dt_string + ',' \
        + x['origin_addresses'][1] + ',' \
        + x['destination_addresses'][1] + ',' \
        + str(x['rows'][1]['elements'][1]['distance']['value']) + ','  \
        + str(x['rows'][1]['elements'][1]['duration']['value'])
    print(string)
    encoded_string = string.encode("utf-8")

    bucket_name = "traffic-storage"
    file_name = dt_string + ".csv"
    s3_path = "" + file_name

    # Store csv file
    try: 
        s3 = boto3.resource("s3")
        s3.Bucket(bucket_name).put_object(Key=s3_path, Body=encoded_string)
    except :
        print("Unexpected error:", sys.exc_info()[0])
    return {
        'statusCode': 200,
        'body': json.dumps(x )
    }
#lambda_handler(0,0)