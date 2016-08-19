from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import sys
import os
import time
import json
import yaml
import s3_utils
from utils import rel_path

company = 'AMAZON'
stock = '$AMZN'
DIRECTORY = os.path.dirname(os.path.abspath(__file__))
json_data = {}
class StdOutListener(StreamListener):

    def __init__(self, api=None):
        super(StdOutListener, self).__init__()
        self.num_tweets = 0
        self.filename = company+'rongbin.dashboard'+time.strftime("%Y%m%d%H%M%S")+'.txt'
        self.conn = s3_utils.S3Connection.from_yaml(rel_path('config/cred.yaml'), 'aws')

    def on_data(self, data):
        global json_data
    	data = json.loads(data)
        if 'text' in data:
            text = data['text']
            created_at = data['created_at']
            record = {}
            record['Text'] = text
            record['Created at'] = created_at
            record['Company'] = company
            record['Stock'] = stock
            self.num_tweets += 1
            if self.num_tweets < 3000:
                print 'Saving to json'
                print record
                json_data[str(self.num_tweets)] = record
                print json_data
            else:
                print 'Saving to local file'
                with open(self.filename,'w') as tf:
                    json.dump(json_data, tf)
                json_data = {}
                print 'Uploading to S3'
                self.conn.upload_file(self.filename, self.filename, company)
                print 'Uploading to S3 Done'
                os.remove(self.filename)
                self.num_tweets = 0
                self.filename = company+'rongbin.dashboard'+time.strftime("%Y%m%d%H%M%S")+'.txt'
        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    tweet_cred = yaml.safe_load(file(rel_path('config/cred.yaml')))['twitter']
    auth = OAuthHandler(tweet_cred['consumer_key'], tweet_cred['consumer_secret'])
    auth.set_access_token(tweet_cred['access_token'], tweet_cred['access_token_secret'])
    stream = Stream(auth, l)
    stream.filter(track=[company, stock])




