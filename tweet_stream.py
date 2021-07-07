import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

access_token = "1288431932-4VI3EA7vpL7FrnKnu61tyYvMIVy7Bh3rDA6ulqS"
access_secret =  "krPnYxZgtPFAXnvMSfuw3dOQTvQ5dSAsdyCfq6wDx6UXn"
consumer_key =  "1Lj8DS1pWpS1FhnqZTBVjunEU"
consumer_secret =  "DVtoIgkxXPQa7xzGKa79xc8STxQn444kfLOdTuICD13Wr3VQoH"


class TweetsListener(StreamListener):
    def __init__(self, csocket):
        self.client_socket = csocket
    def on_data(self, data):
        try:
            msg = json.loads( data )
            self.client_socket.send( msg['text'].encode('utf-8') )
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True
    def on_error(self, status):
        print(status)
        return True
    
    
def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['guitar'])
    

    s = socket.socket()
    host = "0.0.0.0"
    port = 5555
    s.bind1
    s.listen(5)
    c, addr = s.accept()
    sendData(c)