import sys
import kafka
import re
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

batch_durations = 2
input_topic = 'hits'
output_topic = 'output'
broker = 'localhost:9092'

kafka_client_cache = None

# If any one IP sends more than threshold * mean requests in any 
# batch, we mark that IP as an attacker. I have found that 3 works well
# with the test data. 
threshold = 3

def parse_log(log): 
    pattern = re.compile(r'^([0-9.]+)\s([w.-]+)\s([w.-]+)\s(\[.*\])\s"(.*)" (\d*) (\d*) "(.*)" "(.*)"')
    m=pattern.match(log[1])
    return m.group(1)

def time_to_minutes(row):
    return (row[0]/60, row[1])

def output(ip, count):
    global kafka_client_cache
    if not kafka_client_cache:
        kafka_client_cache = kafka.KafkaClient(broker)
    producer = kafka.SimpleProducer(kafka_client_cache)
    producer.send_messages(output_topic, str(ip))

def processRDD(rdd):
    if (rdd.count() == 0): return
    ips_with_counts = (rdd.map(parse_log)
     .map(lambda x: (x, 1))
     .reduceByKey(lambda a, b: a+b))

    total = ips_with_counts.map(lambda (ip, count): count).reduce(lambda a, b: a+b)
    mean = float(total) / ips_with_counts.count()
     
    bad_ips = (ips_with_counts
     .filter(lambda (ip, count): count > mean * threshold)) 
 
    bad_ips.foreach(lambda (ip, count): output(ip, count))

def main():
    sc = SparkContext(appName = "IntrusionDetector")
    ssc = StreamingContext(sc, batch_durations)

    kvs = KafkaUtils.createDirectStream(ssc, [input_topic], {"metadata.broker.list": broker})
    kvs.foreachRDD(processRDD)
    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main() 
