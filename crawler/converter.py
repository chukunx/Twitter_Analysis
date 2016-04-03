import sqlite3 as lite
import os,sys
from contextlib import nested

# encoding problem
reload(sys)
sys.setdefaultencoding('utf-8')

directoryForDB = "./data"
directoryForHB = "./data/csv"
if not os.path.exists(directoryForDB):  # create a new one if it is not exist
    os.makedirs(directoryForDB)
if not os.path.exists(directoryForHB):  # create a new one if it is not exist
    os.makedirs(directoryForHB)

fileName = "/twitter_part1"

DB = directoryForDB + fileName + ".db"
HB = directoryForHB + fileName + ".csv"

with nested(lite.connect(DB), open(HB, "a")) as (con, hb):
    cur = con.cursor()
    tweets = cur.execute("SELECT * FROM tweets").fetchall()
    con.commit()

    current = 0
    for tweet in tweets:
        current = current + 1
        print """%d: %s, %s, %s""" % (current, tweet[1], tweet[2], tweet[6])
        hb.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (tweet[0],tweet[1],tweet[2],tweet[3],tweet[4],tweet[5],tweet[6].decode('utf8')))
