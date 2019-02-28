from tweepy import OAuthHandler
from config import vars
import tweepy
from google.cloud import pubsub_v1
import datetime
import base64
import json

def publish(client, pubsub_topic, data_lines):
    """Publish to the given pubsub topic."""
    messages = []
    for line in data_lines:
        messages.append({'data': line})
    body = {'messages': messages}
    print("BODY IS {}".format(body))
    str_body = json.dumps(body)
    print(type(str_body))
    print("String_body is {}".format(str_body))
    data = base64.urlsafe_b64encode(bytearray(str_body, 'utf8'))
    print("DATA IS {}".format(data))
    client.publish(topic=pubsub_topic, data=data)

class MyStreamListener(tweepy.StreamListener):
    client = pubsub_v1.PublisherClient()
    pubsub_topic = client.topic_path('sumitrauniyar', 'twitter_streaming')
    count = 0
    tweets = []
    batch_size = 15
    total_tweets = 100

    def write_to_pubsub(self, tweets):
        publish(self.client, self.pubsub_topic, tweets)

    def on_status(self, status):

        text = status.text
        self.tweets.append(text)

        if len(self.tweets) >= self.batch_size:
            #self.client.publish(topic=self.pubsub_topic, data=base64.urlsafe_b64encode(bytearray(self.tweets, 'utf8')))
            self.write_to_pubsub(self.tweets)
            print(self.tweets)
            self.tweets = []

        self.count += 1
        if self.count >= self.total_tweets:
            return False
        if (self.count % 5) == 0:
            print("count is: {} at {}".format(self.count, datetime.datetime.now()))
        return True

    def on_error(self, status_code):
        print(status_code)


auth = OAuthHandler(vars.CONSUMER_KEY, vars.CONSUMER_SECRET)
auth.set_access_token(vars.ACCESS_TOKEN, vars.ACCESS_SECRET)
api = tweepy.API(auth)
myStreamListener = MyStreamListener()
stream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
stream.filter(track=["#Pulwama"])
