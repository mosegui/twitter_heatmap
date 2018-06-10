#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 10 17:37:36 2018

@author: dmg
"""

import json

def retrieve_twitter_API_credentials(twitter_API_json_filename):
    """
     The file 'twitter_API_credentials.json' is listed in .gitignore
     and therefore not pushed to into the GitHub repository. For illustrative
     purposes of the JSON layout, the file 'dummy_twitter_API_credentials.json' 
      is added.
    """

    with open(twitter_API_json_filename) as twitter_credentials:
        twitterID = json.load(twitter_credentials)
        
    return twitterID['consumer_key'], twitterID['consumer_secret'], twitterID['access_token'], twitterID['access_secret']