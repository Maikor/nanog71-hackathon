import signal, yaml
import time, os, sys, ipaddress, pdb, argparse
from functools import partial
import logging, logging.handlers
from rshuttle import RouteShuttle, logger
from plugins.openbmp import OpenBMPPlugin, ROUTE_EVENTS
import redis, ast
from confluent_kafka import Consumer, KafkaError
import threading

CURRENT_EVENT=0
EXIT_FLAG = False

# POSIX signal handler to ensure we shutdown cleanly
def handler(shuttler, consumer, signum, frame):
    global EXIT_FLAG

    if not EXIT_FLAG:
        EXIT_FLAG = True
        logger.info("Cleaning up...")
        shuttler.slcleanup()
        logger.info("Closing the Kafka consumer")    
        consumer.close()
        os._exit(0)


def event_listener():
    # connect and bind to topics
    print "Connecting to kafka... takes a minute to load offsets and topics, please wait"
    consumer = Consumer({'bootstrap.servers': results.bootstrap_server, 'group.id': 'router_client'+str(time.time()),
                         'client.id': 'router_client'+str(time.time()),
                         'default.topic.config': {'auto.offset.reset': 'largest',
                                                  'auto.commit.interval.ms': 1000,
                                                  'enable.auto.commit': True }})
    consumer.subscribe(["event"])
    print "Waiting for event messages"

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                # Error or event
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.error('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    # Error
                    raise KafkaException(msg.error())
            else:
                # Push the routes to the route shuttle queue
                print "Event Data is"
                print msg.value()
                global CURRENT_EVENT
                CURRENT_EVENT=int(msg.value())
    except:
         print "Catching exception" 


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--server-ip', action='store', dest='server_ip',
                    help='Specify the IOS-XR GRPC server IP address', required=True)
    parser.add_argument('-p', '--server-port', action='store', dest='server_port',
                    help='Specify the IOS-XR GRPC server port', required=True)
    parser.add_argument('-b', '--bootstrap-server', action='store', dest='bootstrap_server',
                    help='Specify hostname of the kafka cluster', required=True)
    parser.add_argument('-r', '--redis-host', action='store', dest='redis_host',
                    help='Specify the redis server host', required=True)
    parser.add_argument('-v', '--verbose', action='store_true',
                    help='Enable verbose logging')


    results = parser.parse_args()
    if results.verbose:
        logger.info("Starting verbose debugging")
        logging.basicConfig()
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)


    if results.server_ip and results.server_port:
        server_ip = results.server_ip
        server_port = int(results.server_port)



    if results.redis_host is None:
       raise ValueError("Redis Hostname not specified, bailing out")
    else:
       redisClient = redis.StrictRedis(host=results.redis_host)



    # Instantiate the OpenBMPPlugin Class
    openbmp = OpenBMPPlugin()

    # Create the RouteShuttle Object to start internal threads to process incoming routes
    # Also load the NetlinkPlugin class

    shuttler = RouteShuttle(plugin=openbmp, vrf='default', server_port=server_port, server_ip=server_ip)

    # Register against the vrf that is expected to be programmed by the incoming routes.
    shuttler.register_vrf()



    # Now listen to kafka for a live stream of routes


    thread = threading.Thread(target=event_listener, args=())
    thread.daemon = True
    thread.start()

    # connect and bind to topics
    print "Connecting to kafka... takes a minute to load offsets and topics, please wait"
    consumer = Consumer({'bootstrap.servers': results.bootstrap_server, 'group.id': 'router_client'+str(time.time()),
                         'client.id': 'router_client'+str(time.time()),
                         'default.topic.config': {'auto.offset.reset': 'largest', 
                                                  'auto.commit.interval.ms': 1000,
                                                  'enable.auto.commit': True }})
    consumer.subscribe(["business_logic"])
    print "Now consuming/waiting for messages..."
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                # Error or event
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.error('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    # Error
                    raise KafkaException(msg.error())
            else:
                # Push the routes to the route shuttle queue
                route = ast.literal_eval(msg.value())
                if CURRENT_EVENT:
                    print route['monitor bytes out']['action']
                    shuttler.rtQueue.put(route['monitor bytes out']['action'])
                else:
                    route['monitor bytes out']['action']['paths']['event'] = 'del'
                    print route['monitor bytes out']['action']
                    shuttler.rtQueue.put(route['monitor bytes out']['action'])            
    except KeyboardInterrupt:
        logger.error('%% Aborted by user\n')
 



    # Register our handler for keyboard interrupt and termination signals
    signal.signal(signal.SIGINT, partial(handler, shuttler, consumer))
    signal.signal(signal.SIGTERM, partial(handler, shuttler, consumer))


    # The process main thread does nothing but wait for signals
    signal.pause()
