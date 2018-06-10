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
import threading
import sys
import login_twitter

logging.basicConfig(level = logging.INFO)
logger = logging.getLogger('heatmap')

############################################################################################################################3
            
class pandas_to_sql(threading.Thread):
    def __init__(self):
        super().__init__()
        self.isDaemon
        self.event = threading.Event()
    
    def create_sql_database (self, table_name, db_filename = 'default_sql_dbname.db', on_memory=True):
        """
        creates an SQLite database and returns the connection and the cursor to
        interact with it
        """
        
        if on_memory == True:
            conn = sqlite3.connect(':memory:')
        elif on_memory == False:
            conn = sqlite3.connect(db_filename)
        else:
            logger.debug('the variable "on_memory" is a boolean variable')
            
        c = conn.cursor()
        c.execute('CREATE TABLE {} ('.format(table_name) + \
                'timestamp REAL,' + \
                'latitude REAL,' + \
                'longitude REAL,' + \
                'language TEXT,' + \
                'country TEXT,' + \
                'country_code TEXT,' + \
                'place_name TEXT,' + \
                'place_type TEXT)')
        
        conn.commit()
        
        return c, conn
    
    def run(self):
        
        global dummy_df
        
        cursor, connection = self.create_sql_database('tweets', db_filename= 'lalala.db', on_memory = False)
        
        while not self.event.is_set():
            time.sleep(9)
            logger.info('********************************************************************************************len(dummy_df): {}'.format(len(dummy_df)))
            
            dummy_df.to_sql('tweets', connection, if_exists = 'append', index = False)
                        
            dummy_df = pd.DataFrame([], columns=['Timestamp',
                                   'Longitude',
                                   'Latitude',
                                   'Language',
                                   'Country',
                                   'Country_Code',
                                   'Place_Name',
                                   'Place_Type'])
            
            logger.warning('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ DATA MOVED!!')
            
        if self.event.is_set():
            logger.info('exiting...')
            connection.close()
            sys.exit()



class tweet_listener(tp.StreamListener):
         
    def __init__(self, timeout):
        super().__init__(self)
        self.starting_time = time.time()
        self.time_limit = timeout

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
                    
                    
                    logger.info('{}, {} {} {} ({})'.format(timestamp[0], place_name[0], 
                                                           country[0], country_code[0], 
                                                           language[0]))
                    
            except:
                pass # tweets with no ['place'] key are internal API messages
            return True
        logger.info('TIME IS UP!!! ({} sec)'.format(self.time_limit))
        return False
    
    def on_error(self, status):
        logger.info(status)

##################################################################################################################3

consumer_key, consumer_secret, access_token, access_secret  = login_twitter.retrieve_twitter_API_credentials('twitter_API_credentials.json')



heatmap_auth = tp.OAuthHandler(consumer_key, consumer_secret)
heatmap_auth.set_access_token(access_token, access_secret)

try:
    tweets_mover = pandas_to_sql()
    tweets_mover.daemon = True # probably unnecessary
    tweets_mover.start()
    
    while True:
        
        tweet_streamer = tp.Stream(auth= heatmap_auth,listener = tweet_listener(300))
        tweet_streamer.filter(locations=[-180,-90,180,90])

except KeyboardInterrupt:
    tweets_mover.event.set()
    
#tweet_streamer.filter(track=['Trump'])








