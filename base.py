from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka.exceptions import NoBrokersAvailableError, UnknownTopicOrPartition
import json
import threading
import logging
import time
import sys
import GeoIP
PYHTONHASHSEED = 0

if sys.argv[1] is "h":
    print "arg1=kafka client\narg2=topic to consume from\narg3=topic to produce to"
    raise SystemExit
try:
    client = KafkaClient(hosts= sys.argv[1])
    topic = client.topics[sys.argv[2]] #Topic to consume from
    nTopic = client.topics[sys.argv[3]] #Topic to produce to
    consumer = topic.get_simple_consumer(
        consumer_group = sys.argv[2],
        auto_offset_reset = OffsetType.LATEST,
        reset_offset_on_start = True)
    producer = nTopic.get_sync_producer()
except NoBrokersAvailableError:
    print "Please enter a valid Kafka Broker"
    raise SystemExit
except UnknownTopicOrPartition:
    print "Please enter a valid topic"
    raise SystemExit

def produceJSON(Dict):
    js = json.dumps(Dict)
    producer.produce(js)

try:
    for message in consumer:
        try:
            nDict = json.loads(message.value) #loads current json to dict
#
#    Magic goes here
#
            produceJSON(nDict)
        except ValueError:
            pass
except KeyboardInterrupt:
    print "\nQuitting..."
    raise SystemExit
