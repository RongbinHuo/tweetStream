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

company_info = yaml.safe_load(file(rel_path('config/cred.yaml')))['many_company']
companys = company_info['company_name']
stocks = company_info['stock']
DIRECTORY = os.path.dirname(os.path.abspath(__file__))
json_data = {}
class StdOutListener(StreamListener):

    def __init__(self, company, filename, time_end, api=None):
        super(StdOutListener, self).__init__()
        self.num_tweets = 0
        self.company = company
        # self.filename = company+'rongbin.dashboard'+time.strftime("%Y%m%d%H%M%S")+'.txt'
        self.filename = filename
        self.time_end = time_end
        # self.conn = s3_utils.S3Connection.from_yaml(rel_path('config/cred.yaml'), 'aws')

    def on_data(self, data):
        global json_data
    	data = json.loads(data)
        if 'text' in data and time.time()<self.time_end:
            text = data['text']
            created_at = data['created_at']
            record = {}
            record['Text'] = text
            record['Created at'] = created_at
            record['Company'] = self.company
            record['Stock'] = stock
            json_data[str(self.num_tweets)] = record
            self.num_tweets += 1
            return True
        else:
            return False

    def on_error(self, status):
        print status


if __name__ == '__main__':
    company_ary =  companys.split(",")
    stock_ary = stocks.split(",")
    company_len = len(company_ary)
    conn = s3_utils.S3Connection.from_yaml(rel_path('config/cred.yaml'), 'aws')
    start_index = 0
    while start_index<company_len:
        company = company_ary[start_index].upper()
        stock = stock_ary[start_index].upper()
        print "Collecting data for company "+company
        time_end = time.time() + 60 * 15
        filename = company+'rongbin.dashboard'+time.strftime("%Y%m%d%H%M%S")+'.txt'
        l = StdOutListener(company=company,filename=filename,time_end=time_end)
        tweet_cred = yaml.safe_load(file(rel_path('config/cred.yaml')))['twitter']
        auth = OAuthHandler(tweet_cred['consumer_key'], tweet_cred['consumer_secret'])
        auth.set_access_token(tweet_cred['access_token'], tweet_cred['access_token_secret'])
        stream = Stream(auth, l)
        stream.filter(track=[company, stock])
        with open(filename,'w') as tf:
            json.dump(json_data, tf)
        conn.upload_file(filename, company+'/'+filename)
        os.remove(filename)
        json_data = {}
        start_index = start_index+1
        if start_index == company_len:
            start_index = 0




