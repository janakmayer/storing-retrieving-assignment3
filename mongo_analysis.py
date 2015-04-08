import pymongo
import operator

db = pymongo.MongoClient().project

print '%s tweets stored in both files' % db.tweets.count()


# Who tweeted the most during the conference?

results = db.tweets.aggregate(
    {"$group":{"_id":"$user_id", "sum":{"$sum":1}}}
)
result_tuples = sorted([(result[u'_id'], result[u'sum']) for result in results[u'result']],
                       key=operator.itemgetter(1), reverse=True)
max_tweeter_id = result_tuples[0][0]
max_tweeter = db.tweets.find_one({"user_id": max_tweeter_id})['user_name']

print '%s tweeted the most during the conference' % max_tweeter

#What were the top 10 hash tags used?

results = db.tweets.aggregate([
    {"$unwind": "$hashtags" },
    {"$group":{"_id":"$hashtags", "sum":{"$sum":1}}}
])

result_tuples = sorted([(result[u'_id'], result[u'sum']) for result in results[u'result']],
                       key=operator.itemgetter(1), reverse=True)
top_10_hashtags = [result[0] for result in result_tuples[:10]]
print 'The top 10 hashtags were: %s' %top_10_hashtags

# For a particular hour, how many tweets were produced?
# Please provide answers for the hours 9:00+01:00 through 16:00+01:00 on both days.

hours = range(9, 16)
days = ['2015-02-14', '2015-02-15']
for day in days:
    for hour in hours:
        print '%s tweets in %s oclock hour on %s' %(
            db.tweets.find({"created_at": {"$gte": str(day) + ' ' + str(hour+1) + ':00',
                                           "$lt": str(day) + ' ' + str(hour+2) + ':00'}}).count(),
            hour,
            day)

    # hour+1 is adjustment for CET

