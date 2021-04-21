# Databricks notebook source
# MAGIC %pip install spotipy tweepy azure-eventhub

# COMMAND ----------

import spotipy
import json
from spotipy.oauth2 import SpotifyClientCredentials

class SpotifyAPI:
    # create connection to Spotify API
    def __init__(self):
        client_id = '78ef2c1930e347f2a527ea4e2f08b6e9'
        client_secret = '0510098790d149e8af6b880782f6f19b'
        client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
        self.sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    # extract artist name by track id
    def get_track_information(self,id):
        try:
            artist = self.sp.track(id)['artists'][0]['name']
            return artist
        except:
            return None

# COMMAND ----------

import sys
import json
import tweepy
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
from azure.eventhub import EventHubProducerClient, EventData
import time
import asyncio
import os
import uuid
import datetime
import random
import re
from datetime import datetime


#override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):

    def on_data(self, data):
        CONNECTION_STR = 'Endpoint=sb://msft2eventhubs.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SHOSHMrAUaMckISw6q+3LSBBKsi9gGCxSDLUr4YX/R8='
        EVENTHUB_NAME = 'sample-eventhub'
        start_time = time.time()

        producer = EventHubProducerClient.from_connection_string(
            conn_str=CONNECTION_STR,
            eventhub_name=EVENTHUB_NAME
        )
        with producer:
            # Producer produces data for consumer
            json_object = json.loads(data)
            # Filter tweets so only tweets containing link are sent to kafka producer
            # Send only created at and contained urls of the tweets
            if len(json_object['entities']['urls']) > 0:
                data = {'created_at':json_object['created_at'], 'expanded_url':json_object['entities']['urls'][0]['expanded_url']}
                data = json.dumps(data)
                #print(data)
                
                # extract track id for each incoming tweet and use the track id to get artist name using Spotify API
                tweets = json.loads(data)
                #tweets.count().map(lambda count: "Tweets in this batch: %s" % count).pprint()
                #tweets.pprint()
                
                tweet = {"track_id": get_track_id(tweets["expanded_url"]), "created_at": tweets["created_at"]}
                #tweet = json.dumps(tweet)
                #print(tweet)
                
                if tweet["track_id"]!=False:
                    track_id = tweet["track_id"]
                   # print(track_id)
                    #track_ids.pprint()
                    #track_ids.count().map(lambda count: "Tracks in this batch: %s" % count).pprint()
                
                    Spotify = SpotifyAPI()

                    track = {"artist": Spotify.get_track_information(tweet["track_id"]), "created_at": tweet["created_at"]}
                    
                    
                    created_at = datetime.strftime(datetime.strptime(track["created_at"],'%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%dT%H:%M:%S')
                    #print(created_at)
                    
                    
                    eventhub_data = {"created_at": created_at, "track_id": track_id}
                        
                    if track["artist"]!=None:
                        artist = track["artist"]
                        #print(artist)
                        eventhub_data["artist"] = artist
                        eventhub_data = json.dumps(eventhub_data)
                        print(eventhub_data)
                        event_data_batch = producer.create_batch()
                        event_data = EventData(eventhub_data)
                        try:
                            event_data_batch.add(event_data)
                        except ValueError:
                            producer.send_batch(event_data_batch)
                            event_data_batch = producer.create_batch()
                            event_data_batch.add(event_data)
                        if len(event_data_batch) > 0:
                            producer.send_batch(event_data_batch)
                        print("Send messages in {} seconds.".format(time.time() - start_time))
                    

        return True

    def on_error(self, status):
        print(status)
        return True

def get_track_id(urls):
        if(len(urls))>0:
            track_id = re.findall('(?<=track\/)[^.?]*',urls)
            if track_id:
                return track_id[0]
            return False
        else:
            return False

      
if __name__ == '__main__':
    topic = sys.argv[1]
    word_filter = ['spotify com']
    language_filter = ['en']

    api_key = 'kFn1mQt4jqZWr0K8h1OdU16Ka'
    api_secret = 'iyfsrGhCO8bf0BPQD1ijL7ErHoCGY2rlSWBrpqKPfwFDxmIPlN'
    token = '4567229962-e8dIBvZdR00Ee1m1D4r8dza5wURUcbfi68WH063'
    token_secret = 'zW56Sa4reb9vfoaFmwkVqxagPKzjsLuuMSGL4pPX9Af6O'
            
    auth = OAuthHandler(api_key, api_secret)
    auth.set_access_token(token, token_secret)

    twitter_stream = tweepy.Stream(auth, MyStreamListener())
    twitter_stream.filter(languages=language_filter, track=word_filter)
    
    

