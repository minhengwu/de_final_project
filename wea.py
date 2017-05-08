import json
import boto3
import yaml
import os
import requests
import time
from kafka import KafkaProducer

def fire_conn():
    cred = yaml.load(open(os.path.expanduser('credentials.yaml')))
    aws_access_key_id = cred['default']['aws_access_key_id']
    aws_secret_access_key = cred['default']['aws_secret_access_key']
    client = boto3.client('firehose',region_name = 'us-east-1',aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key)
    return client


if __name__ == '__main__':
    conn = fire_conn()
    producer = KafkaProducer(bootstrap_servers= 'localhost:9092', value_serializer = lambda v:json.dumps(v).encode('utf-8'))
    while True:
        r = requests.get('http://api.openweathermap.org/data/2.5/weather?q=Sanfrancisco,us&appid=108f5a7204a479319be0f08798284333')
        if r.status_code == 200:
            conn.put_record(DeliveryStreamName = 'finalprojectweather', Record = {'Data':r.text + '\n'})
            producer.send('weather', r.text)
        b = requests.get('http://api.openweathermap.org/data/2.5/weather?q=Seattle,us&appid=108f5a7204a479319be0f08798284333')
        if r.status_code == 200:
            conn.put_record(DeliveryStreamName = 'finalprojectweather', Record = {'Data':b.text + '\n'})
            producer.send('weather', b.text)
        time.sleep(60)
