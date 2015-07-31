import kafka, time

kafka_client = kafka.KafkaClient('localhost:9092')
consumer = kafka.SimpleConsumer(kafka_client, 'no_group', 'output')

kafka_client.ensure_topic_exists('output')
blocked_ips = set()

i=0
while True:
    message = consumer.get_message()
    if message:
        ip = message.message.value
        blocked_ips.add(ip)
        if (i % 10) == 0:
            print len(blocked_ips), "total, latest is", ip
            with open('blocklist', 'w') as f:
                for blocked_ip in blocked_ips:
                    f.write(blocked_ip + '\n')
        i += 1
    else:
        print "Queue is empty, blocklist contains", len(blocked_ips), "entries"
        time.sleep(1)
