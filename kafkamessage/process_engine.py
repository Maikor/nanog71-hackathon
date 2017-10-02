# Purpose of the code is to talk to and listen from Kafka.
# Listen to the topic on the Kafka bus, pass it on to the code to check if it matches the threshold.
# If it matches, open a event on the Kafka bus.


from confluent_kafka import Consumer, Producer, KafkaError
import json

class TalkToKafka(object):
    def __init__(self, topic):
        self.topic = topic
	self.monitoring_data = {}
        self.bootstrapserver = 'localhost:9092'

    def kafka_push(self, message_to_dump, custom_topic=None):
        if custom_topic is not None:
            topic = custom_topic
        else:
            topic = self.topic

        producer = Producer({'bootstrap.servers': self.bootstrapserver})
        producer.produce(topic, value=json.dumps(message_to_dump))
        producer.poll(0)
        producer.flush()

    def kafka_pull(self):

        consumer_settings = {
            'bootstrap.servers': self.bootstrapserver,
            'group.id': 'mygroup',
            'client.id': 'client-1',
            'enable.auto.commit': True,
            'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'largest',
                                     'auto.commit.interval.ms' : 1000,
                                     'enable.auto.commit' : True}
        }

        print self.topic
        c = Consumer(consumer_settings)
        c.subscribe([self.topic])
        try:
            while True:
                msg = c.poll(0.1)
                if msg is None:
                    continue
                elif not msg.error():
                    #print('Received message: {0}'.format(msg.value()))
                    print msg.value()
		    listen_variable = json.loads(msg.value())
               	    
		    print (listen_variable)
                    try:
		        for interface in listen_variable['interfaces']:
                            if interface['interface-name'] == "GigabitEthernet0/0/0/2":
                                if int(interface['total-bytes-transmitted']) > 4800:
                            	    print 'threshold exceeded'
                                    print "message_to_dump = 1"
				    self.kafka_push(message_to_dump=1, custom_topic="event")
                                else:
                                    print "threshold not exceeded"
                                    print "message_to_dump = 0"
                                    self.kafka_push(message_to_dump=0, custom_topic="event")
                    except Exception as e:
                            print "Error while parsing data"
                            print e
                elif msg.error().code() == KafkaError._PARTITION_EOF:
                    print('End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
                else:
                    print('Error occured: {0}'.format(msg.error().str()))

        except KeyboardInterrupt:
            pass

        finally:
            c.close()
