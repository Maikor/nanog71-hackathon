import yaml
from process_engine import TalkToKafka
import time
# Read YAML file
x = TalkToKafka("business_logic")

while True:
    with open("business_logic.yaml", 'r') as stream:
        data_loaded = yaml.load(stream)
        print(data_loaded)
        x.kafka_push(message_to_dump=data_loaded)
        time.sleep(5)
