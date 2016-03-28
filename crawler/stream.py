from TwitterAPI import TwitterAPI, TwitterRequestError, TwitterConnectionError
import json
import sqlite3 as lite
import urllib
import os
from datetime import datetime
import logging  

logger = logging.getLogger('mylogger')  
logger.setLevel(logging.DEBUG)  
  
fh = logging.FileHandler('test.log')  
fh.setLevel(logging.DEBUG)  
   
ch = logging.StreamHandler()  
ch.setLevel(logging.DEBUG)  

formatter = logging.Formatter('%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s')  
fh.setFormatter(formatter)  
ch.setFormatter(formatter)  
  
logger.addHandler(fh)  
logger.addHandler(ch)  

with open('./config.json') as config_file:
    config = json.load(config_file)
with open('./params.json') as params_file:
    params = json.load(params_file)

api = TwitterAPI(config['keys']['consumer_key'],
                 config['keys']['consumer_secret'],
                 config['keys']['access_token_key'],
                 config['keys']['access_token_secret'])

track = params['search']['track']
track_list = ','.join(track)
states = {}
states = params['states']

directoryForDB = "./data"
if not os.path.exists(directoryForDB):  # create a new one if it is not exist
    os.makedirs(directoryForDB)

directoryForDB = directoryForDB + "/twitter.db"
con = lite.connect(directoryForDB)
with con:
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS tweets")
    cur.execute("CREATE TABLE tweets(user_id TEXT, screen_name TEXT, tweet_id TEXT, user_loc TEXT, hashtags TEXT, created_at TEXT, tweet_text TEXT)")
    while True:
    	try:
    	    r = api.request('statuses/filter', {'track': track_list})
    	    iterator = r.get_iterator()
            total = 0
            geo_enabled = 0
            location = 0
            correct_location = 0
    	    for tweet in iterator:
    	        if 'text' in tweet:
                    total = total +1
                    if tweet['user']['geo_enabled']:
                        geo_enabled = geo_enabled +1
                        if tweet['user']['location'] is not None:
                            location = location +1
                            short_states = tweet['user']['location'][-2:]
                            if short_states in states.keys():
                                correct_location = correct_location + 1
                                length = len(tweet['entities']['hashtags'])
                                hashtags = ""
                                if  length != 0:
                                    for index in range(length):
                                        hashtags = hashtags + tweet['entities']['hashtags'][index]['text'] + ","
                                    hashtags = hashtags[:-1]
    	                        print """%d, %s, %s, %d, %s\n * %s""" % (correct_location, short_states, tweet['user']['screen_name'], length, hashtags, tweet['text'].replace("\n",""))
    	                        insert_stat = 'INSERT INTO tweets VALUES(?,?,?,?,?,?,?)'
    	                        parms = (tweet['user']['id_str'], tweet['user']['screen_name'], tweet['id_str'], tweet['user']['location'], hashtags ,tweet['created_at'], tweet['text'].replace("\n",""))
    	                        cur.execute(insert_stat, parms)
    	                        con.commit()
    	        elif 'disconnect' in tweet:
    	            event = tweet['disconnect']
    	            if event['code'] in [2, 5, 6, 7]:
                        logger.debug(event['reason']) 
    	                raise Exception(event['reason'])  
    	            else:
                        information = """Disconnect[code = %d] at %s: %s, re-try request in 5 mins.""" % (event['code'], str(datetime.now()), event['reason'])
    	                print info
    	                time.sleep(300)
                        logger.info(information)
    	                break
    	except TwitterRequestError as e:
            error_message = ''
            if e.status_code == 403:
            	error_message = 'Forbidden'
            elif e.status_code == 404:
            	error_message = 'Unknown'
            elif e.status_code == 406:
            	error_message = 'Not Acceptable'
            elif e.status_code == 413:
            	error_message = 'Too Long'
            elif e.status_code == 416:
            	error_message = 'Range Unacceptable'
            elif e.status_code == 420:
            	error_message = 'Rate Limited'
            elif e.status_code == 503:
            	error_message = 'Service Unavailable'
            else:
                error_message = 'Success'
            if e.status_code < 500:
                information = """%d at %s: %s, re-try request in 5 mins.""" % (e.status_code, str(datetime.now()), error_message)
                print information
                time.sleep(300)
                logger.debug(information) 
                raise
            else:
                infomation = """%d at %s: %s, re-try request in 5 mins.""" % (e.status_code, str(datetime.now()), error_message)
                print information
                time.sleep(600)
                logger.debug(information)
                pass
    	except TwitterConnectionError:
            information = """%d at %s: Temporary interruption, re-try request in 5 mins.""" % (e.status_code, str(datetime.now()))
    	    print information
    	    time.sleep(300)
            logger.debug(information)
    	    pass
con.close()
