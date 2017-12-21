#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

import io
import json
import os
import sys
import time

from IPython.core.display import clear_output
from tweepy import API
from tweepy.streaming import StreamListener

# Variables Globales
# Configuración acceso api y variables globales
consumer_key = ''
consumer_secret = ''
access_token_key = ''
access_token_secret = ''
# File
stream_language = ['en']
query_list = ['Curie', 'Planck', 'Einstein', 'Bohr', 'Fleming', 'Higgs']
dir_json = './jsons'


class MyListener(StreamListener):

    def __init__(self, api=None):
        self.api = api or API()
        self.counter_lap = 1
        self.counter_tweet = 0
        self.start_time = time.time()
        self.tweet_data = []
        self.status_list = []
        self.star_hour = int(self.get_hour(time.time()))

    def on_status(self, status):
        new_time = time.time()
        new_hour = int(self.get_hour(new_time))
        while self.counter_lap <= 25:
            if self.star_hour == new_hour:
                try:
                    self.status_list.append(status)
                    json_string = json.dumps(status._json, ensure_ascii=False)
                    self.tweet_data.append(json_string)
                    self.counter_tweet += 1
                    clear_output(True)
                    print('Tweets : ' + str(self.counter_tweet)
                          + 'star_time:' + str(self.format_time(self.start_time))
                          + ' -  ' + 'Lap Nº: ' + str(self.counter_lap)
                          + ' -  ' + 'HoraI: ' + str(self.star_hour)
                          , end=' ')
                    time.sleep(2)
                    return True
                except BaseException as e:
                    sys.stderr.write("Error on_data:{}\n".format(e))
                    return True
            else:
                new_file_name = 'tweets_' + str(
                    self.format_time(new_time)) + '-' + str(
                    self.star_hour) + \
                                '.json'
                with io.open(os.path.join(dir_json, new_file_name), 'w',
                             encoding='utf-8') as f:
                    f.write(u'{"tweets":[')
                    f.write(','.join(self.tweet_data))
                    f.write(u']}')
                    self.start_time = time.time()
                    self.counter_lap += 1
                    self.counter_tweet = 0
                    self.tweet_data = []
                    self.star_hour = int(self.get_hour(new_time))
                    return True
        return False

    def on_error(self, status):
        if status == 420:
            print(status)
            return False

    def format_time(self, my_time):
        return time.strftime('%H_%M_%S', time.localtime(my_time))

    def get_hour(self, my_time):
        return time.strftime('%H', time.localtime(my_time))
