from TwitterAPI import TwitterAPI
import json


TRACK_TERM = 'pizza'

with open('./config.json') as config:    
    keys = json.load(config)['keys']

api = TwitterAPI(keys['CONSUMER_KEY'],
                 keys['CONSUMER_SECRET'],
                 keys['ACCESS_TOKEN_KEY'],
                 keys['ACCESS_TOKEN_SECRET'])

r = api.request('statuses/filter', {'track': TRACK_TERM})

for item in r:
    print(item['text'] if 'text' in item else item)