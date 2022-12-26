from datetime import datetime
import sys
import json
import requests
import boto3
import csv

def lambda_handler(event, context):
    api_key ='API_KEY'
    url ='https://maps.googleapis.com/maps/api/distancematrix/json?'
    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
    string = "date,segment,direction,lat1,long1,lat2,long2,adresse1,ville1,pays1,adresse2,ville2,pays2,distance,duree\n"

    with open('traffic.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Aller
            source = row['lat1']+'%2C'+row['long1']
            dest = row['lat2']+'%2C'+row['long2']
            r = requests.get(url + 'origins=' + source + '&destinations=' + dest +'&key=' + api_key)
            x = r.json()
            string = string + dt_string + ',' \
                + row['segment'] + ',Aller,' \
                + row['lat1'] + ',' + row['long1'] + ',' + row['lat2'] + ',' + row['long2'] + ',' \
                + x['origin_addresses'][0] + ',' \
                + x['destination_addresses'][0] + ',' \
                + str(x['rows'][0]['elements'][0]['distance']['value']) + ',' \
                + str(x['rows'][0]['elements'][0]['duration']['value']) + '\n' 
            # Retour
            source = row['lat1']+'%2C'+row['long1']
            dest = row['lat2']+'%2C'+row['long2']
            r = requests.get(url + 'origins=' + dest + '&destinations=' + source +'&key=' + api_key)
            x = r.json()
            string = string + dt_string + ',' \
                + row['segment'] + ',Retour,' \
                + row['lat2'] + ',' + row['long2'] + ',' + row['lat1'] + ',' + row['long1'] + ',' \
                + x['origin_addresses'][0] + ',' \
                + x['destination_addresses'][0] + ',' \
                + str(x['rows'][0]['elements'][0]['distance']['value']) + ',' \
                + str(x['rows'][0]['elements'][0]['duration']['value']) + '\n' 
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

lambda_handler(0,0)