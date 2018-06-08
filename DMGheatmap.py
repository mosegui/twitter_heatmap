#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 12 00:23:19 2018

@author: dmg
"""

import tweepy as tp
import json
import sqlite3
import time
import pandas as pd

#======= TWITTER API CREDENTIALS ==============

consumer_key = 'xo0iRYbtElz7tc6hg4G3DEZQd'
consumer_secret = 'X5wbLZpGsYXTXHnykYyWVXSRjLtI3IGwd0gLeJJLIXwxpQtWAP'
access_token = '834845430932525056-zqPhMFGH51hJeGhAYZi1q5APw4mBy60'
access_secret = 'mkPUHvddNNB1S2fmgEXXHpcHwo5nJKE6sUYIAE5t9i5SC'

#======= CREATE SQL DATABASE ==============

conn = sqlite3.connect('mongo.db')
c = conn.cursor()
c.execute("""CREATE TABLE tweets (
        timestamp REAL,
        latitude REAL,
        longitude REAL,
        language TEXT,
        country TEXT,
        country_code TEXT,
        place_name TEXT,
        place_type TEXT)""")

conn.commit()

#==========================================



#==========================================

class df_storage:
    def __init__(self):        
        self.hot_storage = pd.DataFrame([], columns=['Timestamp',
                                                       'Longitude',
                                                       'Latitude',
                                                       'Language',
                                                       'Country',
                                                       'Country Code',
                                                       'Place Name',
                                                       'Place Type'])

    def df_append(self, decoded_json):
        return self.hot_storage.append({'Timestamp': time.time(),
                                                          'Longitude':(decoded_json['place']['bounding_box']['coordinates'][0][0][0] + decoded_json['place']['bounding_box']['coordinates'][0][2][0])/2.,
                                                          'Latitude':(decoded_json['place']['bounding_box']['coordinates'][0][0][1] + decoded_json['place']['bounding_box']['coordinates'][0][2][1])/2.,
                                                          'Language':decoded_json['lang'],
                                                          'Country':decoded_json['place']['country'],
                                                          'Country Code':decoded_json['place']['country_code'],
                                                          'Place Name':decoded_json['place']['full_name'],
                                                          'Place Type': decoded_json['place']['place_type']
                                                          }, ignore_index=True)
                    

class db_manager:
#    def __init__(self):
#        pass
    
    def insert_single(decoded_json):
        tweet_timestamp = time.time()
        tweet_longitude = (decoded_json['place']['bounding_box']['coordinates'][0][0][0] + decoded_json['place']['bounding_box']['coordinates'][0][2][0])/2.
        tweet_latitude = (decoded_json['place']['bounding_box']['coordinates'][0][0][1] + decoded_json['place']['bounding_box']['coordinates'][0][2][1])/2.
        tweet_language = decoded_json['lang']
        tweet_country = decoded_json['place']['country']
        tweet_country_code = decoded_json['place']['country_code']
        tweet_place_name = decoded_json['place']['full_name']
        tweet_place_type = decoded_json['place']['place_type']

        print (tweet_place_name)

        c.execute("INSERT INTO tweets VALUES (?,?,?,?,?,?,?,?)",
              (tweet_timestamp,tweet_latitude, tweet_longitude, tweet_language,
               tweet_country, tweet_country_code, tweet_place_name, tweet_place_type))        
        conn.commit()
                
        return True
    
#    def insert_chunk(self, decoded_json):
#        tweet_timestamp = time.time()
#        tweet_longitude = (decoded_json['place']['bounding_box']['coordinates'][0][0][0] + decoded_json['place']['bounding_box']['coordinates'][0][2][0])/2.
#        tweet_latitude = (decoded_json['place']['bounding_box']['coordinates'][0][0][1] + decoded_json['place']['bounding_box']['coordinates'][0][2][1])/2.
#        tweet_language = decoded_json['lang']
#        tweet_country = decoded_json['place']['country']
#        tweet_country_code = decoded_json['place']['country_code']
#        tweet_place_name = decoded_json['place']['full_name']
#        tweet_place_type = decoded_json['place']['place_type']
#
#        c.execute("INSERT INTO tweets VALUES (?,?,?,?,?,?,?,?)",
#              (tweet_timestamp,tweet_latitude, tweet_longitude, tweet_language,
#               tweet_country, tweet_country_code, tweet_place_name, tweet_place_type))        
#        conn.commit()
#                
#        return True

class tweet_listener(tp.StreamListener):
    
    def __init__(self, time_limit):
        super().__init__(self)
        self.starting_time = time.time()
        self.time_limit = time_limit
        self.foo = df_storage()
    
    def on_data(self, data):
        while (time.time() - self.starting_time) <= self.time_limit:
            decoded = json.loads(data)
            try:
                if decoded['place'] != None:
#                    db_manager.insert_single(decoded)
                    
                    self.foo.hot_storage = self.foo.df_append(decoded)
    
                    print(self.foo.hot_storage.iloc[-1])
            except:
                pass # tweets with no ['place'] key are internal API messages
            return True
        print ('TIME IS UP!!! ({} sec)'.format(self.time_limit))
        return False
#    
    def on_error(self, status):
        print (status)
      
heatmap_auth = tp.OAuthHandler(consumer_key, consumer_secret)
heatmap_auth.set_access_token(access_token, access_secret)

tweet_streamer = tp.Stream(auth= heatmap_auth,listener = tweet_listener(300))
#tweet_streamer.filter(track=['Trump'])
tweet_streamer.filter(locations=[-180,-90,180,90])
conn.close()







