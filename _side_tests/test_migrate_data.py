#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 10 12:00:02 2018

@author: dmg
"""
import pandas as pd
import threading
import time
import sys

global a
global b

a = pd.DataFrame([], columns=['Datetime'])
b = pd.DataFrame([], columns=['Datetime'])


class move_data(threading.Thread):
    def __init__(self):
        super().__init__()
        self.isDaemon
        self.event = threading.Event()
        
    def run(self):
        global a
        global b
        
        while not self.event.is_set():
            time.sleep(5)
            print('len(a)', len(a))
            b = b.append(a)
            a = pd.DataFrame([], columns=['Datetime'])
            print ('DATA MOOOOVED')
            print ('len(b)', len(b))

        if self.event.is_set():
            print ('GOODBYE')
            sys.exit()
            
try:
    foo = move_data()
    foo.daemon = True
    foo.start()
    
    while True:
        a = a.append(pd.DataFrame([time.time()], columns=['Datetime']))
        print('data appended')
        time.sleep(0.5)
except KeyboardInterrupt:
    foo.event.set()
    