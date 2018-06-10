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
import logging


logging.basicConfig(level = logging.DEBUG)
logger = logging.getLogger('heatmap')

#======= TWITTER API CREDENTIALS ==============

# The file 'twitter_API_credentials.json' is listed in .gitignore
# and therefore not pushed to into the GitHub repository. For illustrative
# purposes of the JSON layout, the file 'dummy_twitter_API_credentials.json' 
#  is added.

with open('twitter_API_credentials.json') as twitter_credentials:
    twitterID = json.load(twitter_credentials)

consumer_key = twitterID['consumer_key']
consumer_secret = twitterID['consumer_secret']
access_token = twitterID['access_token']
access_secret = twitterID['access_secret']

#======= CREATE SQL DATABASE ==============

#conn = sqlite3.connect('mongo.db')
conn = sqlite3.connect(':memory:')
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

                    
class db_manager:
    
    def insert_single(decoded_json):
        tweet_timestamp = time.time()
        tweet_longitude = (decoded_json['place']['bounding_box']['coordinates'][0][0][0] + decoded_json['place']['bounding_box']['coordinates'][0][2][0])/2.
        tweet_latitude = (decoded_json['place']['bounding_box']['coordinates'][0][0][1] + decoded_json['place']['bounding_box']['coordinates'][0][2][1])/2.
        tweet_language = decoded_json['lang']
        tweet_country = decoded_json['place']['country']
        tweet_country_code = decoded_json['place']['country_code']
        tweet_place_name = decoded_json['place']['full_name']
        tweet_place_type = decoded_json['place']['place_type']

        logger.info(tweet_place_name)

        c.execute("INSERT INTO tweets VALUES (?,?,?,?,?,?,?,?)",
              (tweet_timestamp,tweet_latitude, tweet_longitude, tweet_language,
               tweet_country, tweet_country_code, tweet_place_name, tweet_place_type))        
        conn.commit()
                
        return True
    


class tweet_listener(tp.StreamListener):
         
    def __init__(self, time_limit):
        super().__init__(self)
        self.starting_time = time.time()
        self.time_limit = time_limit
#        self.df_springboard = df_storage()
        global dummy_df
        dummy_df = pd.DataFrame([], columns=['Timestamp',
                                   'Longitude',
                                   'Latitude',
                                   'Language',
                                   'Country',
                                   'Country_Code',
                                   'Place_Name',
                                   'Place_Type'])
        
            
    def on_data(self, data):

        while (time.time() - self.starting_time) <= self.time_limit:
            decoded_json = json.loads(data)
            try:
                if decoded_json['place'] != None:
#                    db_manager.insert_single(decoded)
#                    self.df_springboard.hot_storage = self.df_springboard.df_append(decoded)
#                    logger.info(self.df_springboard.hot_storage.iloc[-1])
                    
                    timestamp = time.time(),
                    longitude = (decoded_json['place']['bounding_box']['coordinates'][0][0][0] + decoded_json['place']['bounding_box']['coordinates'][0][2][0])/2.,
                    latitude = (decoded_json['place']['bounding_box']['coordinates'][0][0][1] + decoded_json['place']['bounding_box']['coordinates'][0][2][1])/2.,
                    language = decoded_json['lang'],
                    country = decoded_json['place']['country'],
                    country_code = decoded_json['place']['country_code'],
                    place_name = decoded_json['place']['full_name'],
                    place_type = decoded_json['place']['place_type']
                    
                    
                    global dummy_df
                    dummy_df = dummy_df.append({'Timestamp': timestamp[0],
                                                          'Longitude':longitude[0],
                                                          'Latitude':latitude[0],
                                                          'Language':language[0],
                                                          'Country':country[0],
                                                          'Country_Code':country_code[0],
                                                          'Place_Name':place_name[0],
                                                          'Place_Type': place_type
                                                          }, ignore_index=True)
                    
                    
                    logger.info('{}, {} {} {} ({})'.format(timestamp[0], place_name[0], country[0], country_code[0], language[0]))
                    
            except:
                pass # tweets with no ['place'] key are internal API messages
            return True
        logger.info('TIME IS UP!!! ({} sec)'.format(self.time_limit))
        return False
#    
    def on_error(self, status):
        logger.info(status)
      
heatmap_auth = tp.OAuthHandler(consumer_key, consumer_secret)
heatmap_auth.set_access_token(access_token, access_secret)


tweet_streamer = tp.Stream(auth= heatmap_auth,listener = tweet_listener(300))

# CLASS MOVING ENTRIES FROM THE PANDAS DF TO THE SQL DB FIRES HERE
#while True:
#    dummy_df.to_sql('tweets', conn, if_exists='append', index=False)
#    dummy_df = pd.DataFrame([], columns=['Timestamp',
#                                   'Longitude',
#                                   'Latitude',
#                                   'Language',
#                                   'Country',
#                                   'Country_Code',
#                                   'Place_Name',
#                                   'Place_Type'])
#    logger.warning('++++++++++++++++ EMPTY')
#    time.sleep(9)


#tweet_streamer.filter(track=['Trump'])
tweet_streamer.filter(locations=[-180,-90,180,90])
conn.close()







