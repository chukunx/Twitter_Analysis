from TwitterAPI import TwitterAPI, TwitterRequestError, TwitterConnectionError
import json
import sqlite3 as lite
import urllib
import os
from datetime import datetime


with open('./config.json') as config_file:
    config = json.load(config_file)


api = TwitterAPI(config['keys']['consumer_key'],
                 config['keys']['consumer_secret'],
                 config['keys']['access_token_key'],
                 config['keys']['access_token_secret'])

track = config['search']['track']
track_list = ','.join(track)

directoryForDB = "./data"
if not os.path.exists(directoryForDB):  # create a new one if it is not exist
    os.makedirs(directoryForDB)

directoryForDB = directoryForDB + "/twitter.db"
con = lite.connect(directoryForDB)
with con:
    cur = con.cursor()
    # cur.execute("DROP TABLE IF EXISTS tweets")
    # cur.execute("CREATE TABLE tweets(user_id TEXT, tweet_id TEXT, favorite_count INTEGER, retweet_count INTEGER, created_at TEXT, text TEXT)")
    while True:
    	try:
    	    r = api.request('statuses/filter', {'track': track_list})
    	    iterator = r.get_iterator()
    	    for tweet in iterator:
    	        if 'text' in tweet:
    	            print """%s, %s, %s""" % (r.status_code, tweet['user']['id_str'], tweet['text'])
    	            insert_stat = 'INSERT INTO tweets VALUES(?,?,?,?,?,?)'
    	            parms = (tweet['user']['id_str'],tweet['id_str'],tweet['favorite_count'],tweet['retweet_count'],tweet['created_at'],tweet['text'].replace("\n",""))
    	            cur.execute(insert_stat, parms)
    	            con.commit()
    	        elif 'disconnect' in tweet:
    	            event = tweet['disconnect']
    	            if event['code'] in [2, 5, 6, 7]:
    	                raise Exception(event['reason'])
    	            else:
    	                print """Disconnect[code = %d] at %s: %s, re-try request in 5 mins.""" % (event['code'], str(datetime.now()), event['reason'])
    	                time.sleep(300)
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
                print """%d at %s: %s, re-try request in 5 mins.""" % (e.status_code, str(datetime.now()), error_message)
                time.sleep(300)
                raise
            else:
                print """%d at %s: %s, re-try request in 5 mins.""" % (e.status_code, str(datetime.now()), error_message)
                time.sleep(600)
                pass
    	except TwitterConnectionError:
    	    print """%d at %s: Temporary interruption, re-try request in 5 mins.""" % (e.status_code, str(datetime.now()))
    	    time.sleep(300)
    	    pass
con.close()
