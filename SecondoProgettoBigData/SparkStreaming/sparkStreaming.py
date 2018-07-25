"""
To run
  spark-submit --jar spark-streaming-kafka-0-8-assembly_2.11-2.3.0.jar sparkStreaming.py
"""

#    Print to stdout
from __future__ import print_function
#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    MongoClient
from pymongo import MongoClient
#    json parsing
import json


def sendRecord(tweet):
        connection = MongoClient('localhost',27017)
        db = connection.clusterdb
        tweet_coll = db.sparkStreamingResult
        tweet_coll.insert_one(tweet)
        connection.close()
        print('DONE ...')

def check(tweet):
    keys=["asshole","bastard","dickhead","dumbass","dullard","fuck","block-head","jackass","muthafucka","shitface","slut","moron","bitch","shit"]
    words = tweet['text'].split(" ")
    result = any(k in words for k in keys)
    return (tweet,result)

def to_dict((tweet,b)):
    
    return tweet # Convert input to a format acceptable by `insert_many`, for example with json.loads


if __name__ == "__main__":

    # Create a local StreamingContext with two working thread and batch interval of 1 second
    sc = SparkContext("local[2]", "TweetConsumer")
    ssc = StreamingContext(sc, 10)

    kafkaStream  = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'tweet-output-for-streaming':1})

    parsed = kafkaStream.map(lambda v: json.loads(v[1]))

    # Check bad tweet
    tweets = parsed.map(lambda tweet : check(tweet))\
        .filter(lambda (x,b) : b)\
        .map(to_dict)

    tweets.foreachRDD(lambda rdd: rdd.foreach(sendRecord))

    # Print the bad texts
    tweets.map(lambda x : (x['text'],1)).reduceByKey(lambda x,y: x+y).pprint()

    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate
