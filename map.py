#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 17 14:30:21 2018

@author: dmg
"""
import folium
from selenium import webdriver


#driver = webdriver.Chrome("/home/dmg/Desktop/twitter_heatmap/chromedriver")

m = folium.Map(location=[31.35, 2.1])
m.save("mymap.html")

#driver.get("file:///home/dmg/Desktop/twitter_heatmap/mymap.html")