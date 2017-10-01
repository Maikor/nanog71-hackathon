


# Purpose of the code is to talk to and listen from Kafka.
# Listen to the topic on the Kafka bus, pass it on to the code to check if it matches the threshold.
# If it matches, open a event on the Kafka bus.


from confluent_kafka import Consumer, Producer, KafkaError
import json

print "Asda"


def kafkaWorker(self):
        # With the local Rib ready, push routes to Kafka. This is meant to 
        # serve as a streaming set of routes to router clients which will be
        # kafka consumers. This is NOT a way to resync if the router dies or 
        # router client disconnects - for that sync with the redis database
        # first and then start listening to fresh messages from Kafka for route events. 

        self.rib_producer = Producer({'bootstrap.servers': self.bootstrap_server})

        if self.get_nodes():
            for node in self.nodes.keys():
               
               topic =  self.nodes[node].hash
               
               # fetch localRib routes from Redis, push to Kafka bus
               localRib = ast.literal_eval(self.redis.hget(node, 'localRib'))
               if localRib:
                   for route in localRib:
                       logger.debug(route)
                    #   self.shuttler.rtQueue.put(route) 
                       try:
                           self.rib_producer.produce(topic, value=json.dumps(route), callback=self.delivery_callback)
                           self.rib_producer.poll(0)
                       except BufferError as e:
                           logger.debug('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                           len(self.rib_producer))
                           #  putting the poll() first to block until there is queue space available. 
                           # This blocks for RIB_PRODUCER_WAIT_INTERVAL seconds because  message delivery can take some time
                           # if there are temporary errors on the broker (e.g., leader failover).    
                           self.rib_producer.poll(RIB_PRODUCER_WAIT_INTERVAL*1000) 
      
                           # Now try again when there is hopefully some free space on the queue
                           self.rib_producer.produce(topic, value=json.dumps(route), callback=self.delivery_callback)


                   # Wait until all messages have been delivered
                   logger.debug('%% Waiting for %d deliveries\n' % len(self.rib_producer))
                   self.rib_producer.flush()


bootstrapserver = 'localhost:9092'
topic = 'testing_producer'


def kakfa_push():
	
	route = { '1': '123', '2': '1234'}
	producer = Producer({'bootstrap.servers': bootstrapserver})
	producer.produce(topic, value=json.dumps(route))
	producer.poll(0)
	producer.flush()




kakfa_push()




consumer_settings = {
    'bootstrap.servers': bootstrapserver,
    'group.id': 'mygroup',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}

c = Consumer(consumer_settings)

c.subscribe([topic])

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            print('Received message: {0}'.format(msg.value()))
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            print('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass

finally:
    c.close()





class TalkToKafka(object):

	def __init__(self, topic):
		self.topic = topic


	def kafka_push(self, message_to_dump):
		producer = Producer({'bootstrap.servers': bootstrapserver})
		producer.produce(self.topic, value=json.dumps(message_to_dump))
		producer.poll(0)
		producer.flush()


	def kafka_pull(self):
		c = Consumer(consumer_settings)
		consumer_settings = {
    	'bootstrap.servers': bootstrapserver,
    	'group.id': 'mygroup',
    	'client.id': 'client-1',
    	'enable.auto.commit': True,
    	'session.timeout.ms': 6000,
    	'default.topic.config': {'auto.offset.reset': 'smallest'}
		}

		c = Consumer(consumer_settings)
		c.subscribe([self.topic])
		try:
			while True:
        	msg = c.poll(0.1)
        	if msg is None:
            continue
        elif not msg.error():
            print('Received message: {0}'.format(msg.value()))
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            print('Error occured: {0}'.format(msg.error().str()))

		except KeyboardInterrupt:
    		pass

		finally:
    		c.close()


message = { '1': '123', '2': '1234'}
topic = 'class_test'
x = TalkToKafka(topic)
x.kakfa_push(message)
x.kafka_pull()
