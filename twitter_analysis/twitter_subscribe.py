import time
import json
from google.cloud import pubsub_v1
import base64

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_name}`
subscription_path = subscriber.subscription_path("sumitrauniyar","twitter_subscribe")

def collect_tweets(data):
    tweets = []
    stream = base64.urlsafe_b64decode(data)
    print(stream)
    twraw = json.loads(stream)
    print(twraw)
    twmessages = twraw['messages']
    print(twmessages)
    for message in twmessages:
        tweets.append(message['data'])
    print(tweets)

def callback(message):
    collect_tweets(message.data)
    message.ack()

subscriber.subscribe(subscription_path, callback=callback)

# The subscriber is non-blocking. We must keep the main thread from
# exiting to allow it to process messages asynchronously in the background.
print('Listening for messages on {}'.format(subscription_path))
while True:
    time.sleep(60)