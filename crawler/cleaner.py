import sqlite3 as lite
import re
import os,sys
import csv
from contextlib import nested

# encoding problem
reload(sys)
sys.setdefaultencoding('utf-8')

dataPath = "./data/csv/"
outputPath = "./data/output/"
if not os.path.exists(dataPath):  # create a new one if it is not exist
    os.makedirs(dataPath)
if not os.path.exists(outputPath):  # create a new one if it is not exist
    os.makedirs(outputPath)
dataFile = "twitter_part1.csv"
outputFile = "twitter_part1.input"

with nested(open(outputPath + outputFile, "w+"), open(dataPath + dataFile, "r")) as (out, csvfile):
    # user_Id, screen_name, tweet_id, user_loc, hashtags, created_at, tweet_text
    tweets = csv.reader(csvfile, delimiter='\t')
    total = 0
    count = 0
    ommited = 0
    urlOnly = 0
    for tweet in tweets:
        total = total + 1
        if(len(tweet) == 7):
            tweet_text = tweet[6]
            tweet_text = re.sub(r"^RT ", "", tweet_text)
            tweet_text = re.sub(r"^@[A-Za-z0-9_]{1,15}: ", "", tweet_text)
            found = re.findall(r"http\S+", tweet_text)
            if(len(found)!=0):
                sentence = re.sub(r"http\S+", 'ulr_replmt', tweet_text)
                if(sentence.strip() == ""):
                    urlOnly = urlOnly + 1
                    continue
                else:
                    count = count + 1
                    ## full output
                    out.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (tweet[0],tweet[1],tweet[2],tweet[3],tweet[4],tweet[5],sentence.decode('utf8')))
                    ## user_id, tweet_id, tweets
                    # out.write("%s\t%s\t%s\n" % (tweet[0],tweet[2],sentence.decode('utf8')))
                    ## log to screen
                    print """%d, %d, %d, %d, %s, %s""" % (total, count, urlOnly, ommited, ','.join(found), tweet[1])

        else:
            ommited = ommited + 1
