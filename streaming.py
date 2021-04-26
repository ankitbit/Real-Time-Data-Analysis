from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaClient, KafkaProducer

access_token = "1288431932-4VI3EA7vpL7FrnKnu61tyYvMIVy7Bh3rDA6ulqS"
access_token_secret =  "krPnYxZgtPFAXnvMSfuw3dOQTvQ5dSAsdyCfq6wDx6UXn"
consumer_key =  "1Lj8DS1pWpS1FhnqZTBVjunEU"
consumer_secret =  "DVtoIgkxXPQa7xzGKa79xc8STxQn444kfLOdTuICD13Wr3VQoH"

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send("trump", data.encode('utf-8'))
        print(data)
        return True
    def on_error(self, status):
        print(status)

kafka = KafkaClient()
producer = KafkaProducer()
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track="trump")