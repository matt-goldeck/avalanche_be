import base64
import os

import psycopg2
import requests

MAX_ROW_LIMIT = 10000  # hard limit of heroku free tier


class ParsedTweet(object):
    def __init__(self, raw_obj):
        self.raw_obj = raw_obj

    @property
    def content(self):
        return self.raw_obj.get('text', None)

    @property
    def id(self):
        return self.raw_obj.get('id', None)


class TwitterAPIClient(object):
    def __init__(self):
        self.base_url = 'https://api.twitter.com/'

    def get_request(self, url, params=None):
        get_headers = {
            'Authorization': 'Bearer {}'.format(os.environ.get('ACCESS_TOKEN'))
        }

        resp = requests.get(url, headers=get_headers, params=params)
        resp.raise_for_status()

        return resp

    def collect_popular_topics(self, location_id=None, topic_count=50):
        """Return top five trending topics near location; default to NYC"""
        url = '{}1.1/trends/place.json'.format(self.base_url)

        if not location_id:
            location_id = '2459115'  # New York's WOEID

        params = {
            'id': location_id,
        }

        resp = self.get_request(url, params)
        popular_topics = [topic['query'] for topic in sorted(
            resp.json()[0].get('trends'), key=lambda trend: trend.get('tweet_volume') if trend.get('tweet_volume') else 0, reverse=True)[:topic_count]]

        return popular_topics

    def get_recent_tweets_for_topic(self, topic_query, count=100):
        search_url = '{}1.1/search/tweets.json'.format(self.base_url)
        search_params = {
            'q': topic_query,
            'result_type': 'recent',  # most recent instead of most popular
            'count': count,
            'lang': 'en',  # ask for english only
        }

        resp = self.get_request(search_url, search_params)

        return [ParsedTweet(tweet) for tweet in resp.json().get('statuses')]


class HerokuConnection(object):
    def __init__(self):
        self.connection = psycopg2.connect(os.environ.get('DATABASE_URL'), sslmode='require')
        self.cursor = self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def close(self):
        self.cursor.close()
        self.connection.close()

    def get_stored_tweet_ids(self):
        self.cursor.execute("SELECT twitter_id FROM tweets;")
        return {val[0] for val in self.cursor.fetchall()}

    def insert_tweets(self, tweets):
        tweet_tuples = [(tweet.content, tweet.id) for tweet in tweets]
        query = "INSERT INTO tweets (content, twitter_id) VALUES {};".format(", ".join(["(%s, %s)" for tweet in tweet_tuples]))

        self.cursor.execute(query, [val for tweet in tweet_tuples for val in tweet])
        self.commit()

    def trim_tweets_table(self, to_insert=1000):
        """Check if table close to the 10k record limit. If true, delete enough old tweets to make room for new tweets"""
        query = "SELECT COUNT(*) FROM tweets;"

        self.cursor.execute(query)
        current_count = self.cursor.fetchone()[0]
        space_remaining = MAX_ROW_LIMIT - current_count

        # delete enough rows to make room for rows we're going to insert
        if space_remaining < to_insert:
            to_delete = to_insert - space_remaining
            print("Deleting {} rows from database".format(to_delete))
            query = "DELETE FROM tweets WHERE pk IN (SELECT pk FROM tweets ORDER BY pk asc LIMIT %s);"
            self.cursor.execute(query, (to_delete,))

        self.commit()


def main():
    api_client = TwitterAPIClient()

    # Grab top topics on twitter in NYC
    print("Pulling top topics...")
    topics = api_client.collect_popular_topics()

    # Get map of already stored tweets
    conn = HerokuConnection()
    print("Pulling stored tweet ids...")
    stored_tweet_ids = conn.get_stored_tweet_ids()

    # Grab 100 most recent tweets for each topic
    tweets = []
    for topic in topics:
        print("Pulling tweets for topic: {}".format(topic))
        found_tweets = [tweet for tweet in api_client.get_recent_tweets_for_topic(topic) if tweet.id not in stored_tweet_ids]
        print("Found {} tweets for topic: {}".format(len(found_tweets), topic))

        tweets += found_tweets

    # Make room in DB for tweets
    print("Trimming tweets table...")
    conn.trim_tweets_table(to_insert=len(tweets))

    # Dump them all in the database
    print ("Pushing tweets to database...".format(len(tweets)))
    conn.insert_tweets(tweets)

    conn.close()
    print("Finished inserting {} tweets".format(len(tweets)))


main()
