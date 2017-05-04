import twitter
import yaml
import os
import boto3
import json
from datetime import datetime

credentials = yaml.load(open(os.path.expanduser('/vagrant/twitter.yaml')))
t = twitter.TwitterStream(auth=twitter.OAuth(**credentials['twitter']))


cred = yaml.load(open(os.path.expanduser('/vagrant/credentials.yaml')))
aws_access_key_id = cred['default']['aws_access_key_id']
aws_secret_access_key = cred['default']['aws_secret_access_key']
client = boto3.client('firehose',region_name = 'us-east-1',aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key)

while 1==1:
    itera = t.statuses.sample()
    for tweet in itera:
        client.put_record(DeliveryStreamName='twitter_tweet',Record={'Data': json.dumps(tweet) + '\n'})
