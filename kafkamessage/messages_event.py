from process_engine import TalkToKafka
import time




x = TalkToKafka("event")




while True:

    x.kafka_push(message_to_dump=0)
    time.sleep(5)

    x.kafka_push(message_to_dump=1)
    time.sleep(10)
