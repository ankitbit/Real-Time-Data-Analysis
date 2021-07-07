from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaClient, KafkaProducer


import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

access_token = "1288431932-4VI3EA7vpL7FrnKnu61tyYvMIVy7Bh3rDA6ulqS"
access_token_secret =  "krPnYxZgtPFAXnvMSfuw3dOQTvQ5dSAsdyCfq6wDx6UXn"
consumer_key =  "1Lj8DS1pWpS1FhnqZTBVjunEU"
consumer_secret =  "DVtoIgkxXPQa7xzGKa79xc8STxQn444kfLOdTuICD13Wr3VQoH"

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send("BloodMatters", data.encode('utf-8'))
        print(data)
        return True
    def on_error(self, status):
        print(status)

kafka = KafkaClient()
producer = KafkaProducer()
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
#stream = Stream(auth, l)
#stream.filter(track="BloodMatters"

if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", "target/spark-warehouse") \
        .enableHiveSupport() \
        .getOrCreate
    ssc = StreamingContext(sc, 2)
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a+b)
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()