import yaml
from process_engine import TalkToKafka

# Read YAML file
with open("business_logic.yaml", 'r') as stream:
    data_loaded = yaml.load(stream)

    x = TalkToKafka("business_logic")
    x.kafka_push(message_to_dump=data_loaded)
    x.kafka_pull()
