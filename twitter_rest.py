# This script queries hashtags with #snhu and #southernnewhampshire and stores the
# data into MongoDB

# import necessary libraries
import pymongo
from pymongo import MongoClient
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import datetime

# Values from Twitter required for access
API_KEY = 'QZFotcLwVNoQnNMeTEswXoHtv'
API_SECRET_KEY = 'N2zlIUlVqgeO5W5xm2BwSlsdgiJQX6x200uIu8iCfwscptlDHV'
ACCESS_TOKEN = '1289297493042302976-PHoSflZEWAQiN5CKvKbX8MqwX1kH4Y'
ACCESS_TOKEN_SECRET = 'C7kSnfodD2iERarhnI0dkO6isLLwrLKH7dSxqd5hkDNnh'

# Information needed to connect to MongoDB

client = pymongo.MongoClient("mongodb+srv://hhollee:january9@cs499.7fw1n.mongodb.net/<dbname>?retryWrites=true&w=majority")
db = client.tweets
collection = db.snhu

# Query of extracted data
query = ['#snhu', '#southernnewhampshireuniversity']
count = 100  # limit returns


# Listener class imported from tweepy to stream tweets
class MyStreamListener(tweepy.StreamListener):
    def on_connect(self):
        print("You are connected")

    def on_error(self, status_code):
        # print if error
        print('Error: ' + repr(status_code))
        return False

    # Connect to MongoDB and store data
    def on_data(self, data):
        try:
            client = pymongo.MongoClient(
                "mongodb+srv://hhollee:january9@cs499.7fw1n.mongodb.net/<dbname>?retryWrites=true&w=majority")
            db = client.tweets

            # Decode JSON from Twitter
            twitterjson = json.loads(data)

            # data from tweet to store into database
            tweet_id = twitterjson['id_str']
            text = twitterjson['text']
            posted = twitterjson['created']

            time_stamp = datetime.datetime.strptime(posted, '%a %b %d %H:%M:%S +0000 %Y')
            print(tweet_id + "\n")
             # insert data into collection
            collection.insert(twitterjson)
        except Exception as e:
            print(e)


# Set up listener
auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
listener = MyStreamListener(api=tweepy.API(wait_on_rate_limit=True))
stream = tweepy.Stream(auth=auth, listener=listener)
stream.filter(track=query)



