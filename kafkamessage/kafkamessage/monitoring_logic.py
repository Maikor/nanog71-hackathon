import yaml
from process_engine import TalkToKafka
import time
# Read YAML file
with open("business_logic.yaml", 'r') as stream:
    data_loaded = yaml.load(stream)
    print(data_loaded)
    x = TalkToKafka("business_logic")
<<<<<<< HEAD
    while True:
	x.kafka_push(message_to_dump=data_loaded)
        time.sleep(5)
=======
    x.kafka_push(message_to_dump=data_loaded)
    #x.kafka_pull()


>>>>>>> 207ca55e1721acd759a3776d16c770e61d564d4b
