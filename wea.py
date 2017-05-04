import json
import boto3
import yaml
import os
import datetime
import requests
import time

def fire_conn():
    cred = yaml.load(open(os.path.expanduser('credentials.yaml')))
    aws_access_key_id = cred['default']['aws_access_key_id']
    aws_secret_access_key = cred['default']['aws_secret_access_key']
    client = boto3.client('firehose',region_name = 'us-east-1',aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key)
    return client

# def find_price(stock):
#     r = requests.get('http://finance.google.com/finance/info?client=ig&q=NSE:' + stock)
#     if r == 200:
#         print (r.text)
        #return r.text

# def upload(data):
#     client.put_record(DeliveryStreamName = 'googlefinancefinalproject', Record = data)

if __name__ == '__main__':
    conn = fire_conn()
    while True:
        r = requests.get('http://api.openweathermap.org/data/2.5/weather?q=Sanfrancisco,us&appid=108f5a7204a479319be0f08798284333')
        if r.status_code == 200:
            conn.put_record(DeliveryStreamName = 'finalprojectweather', Record = {'Data':r.text + '\n'})
        b = requests.get('http://api.openweathermap.org/data/2.5/weather?q=Seattle,us&appid=108f5a7204a479319be0f08798284333')
        if r.status_code == 200:
            conn.put_record(DeliveryStreamName = 'finalprojectweather', Record = {'Data':b.text + '\n'})
        time.sleep(60)
