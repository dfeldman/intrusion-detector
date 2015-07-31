#!/usr/bin/python
import kafka, time

kafka_client = kafka.KafkaClient('localhost:9092')
producer = kafka.SimpleProducer(kafka_client)

kafka_client.ensure_topic_exists('hits')

lines = open('apache-access-log.txt').readlines()

i = 0

while True:
    sz = len(lines)
    subset = lines[(((i)*10000) % sz):(((i+1)*10000) % sz)]
    print "Sending ", len(subset), " hits"
    for line in subset:
        producer.send_messages('hits', line)
    time.sleep(1)
    i += 1
