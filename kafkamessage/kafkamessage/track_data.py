import yaml
from process_engine import TalkToKafka
import time
# Read YAML file
x = TalkToKafka("monitor_data")

msg = x.kafka_pull()
