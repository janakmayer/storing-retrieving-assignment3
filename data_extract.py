import json
import pymongo
import time


def store_tweet_data(files, db):
    for f in files:
        with open(f, 'r') as json_file:
            raw_tweets = json.load(json_file)
        for raw_tweet in raw_tweets:
            ts = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(raw_tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
            tweet = {'tweet_id': raw_tweet['id'],
                     'user_id': raw_tweet['user']['id'],
                     'user_name': raw_tweet['user']['name'],
                     'hashtags':[hashtag['text'] for hashtag in raw_tweet['entities']['hashtags']],
                     'created_at': ts}
            db.insert(tweet)

if __name__ == '__main__':
    files = ['prague-2015-02-14.json', 'prague-2015-02-15.json']
    conn = pymongo.MongoClient()
    db = conn.project.tweets
    store_tweet_data(files, db)


