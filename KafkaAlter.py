from pykafka import KafkaClient
from pykafka.common import OffsetType
import json
import threading
import logging
import time
import sys
import GeoIP
PYHTONHASHSEED = 0

if sys.argv[1] is "h" or "-h":
    print "arg1=kafka client\narg2=topic to consume from\narg3=topic to produce to"
    raise SystemExit

geo = GeoIP.open("GeoLiteCity.dat", GeoIP.GEOIP_STANDARD)
client = KafkaClient(hosts= sys.argv[1])
topic = client.topics[sys.argv[2]] #Topic to consume from
nTopic = client.topics[sys.argv[3]] #Topic to produce to
consumer = topic.get_simple_consumer(
    consumer_group = sys.argv[2],
    auto_offset_reset = OffsetType.LATEST,
    reset_offset_on_start = True)
producer = nTopic.get_sync_producer()

try:
    for message in consumer:
        try:
            nDict = json.loads(message.value) #loads current json to dict
            key = nDict.keys() #should put every key from that json into an array
            value = nDict.values()
            ipDict = nDict["ipv4"] #Loads the keys inside ipv4 into a new dict
            geoL = geo.record_by_addr(ipDict["srcAddr"]) #Get Lat/Long from ip
            if geoL is not None:
                nDict['Latitude'] = geoL['latitude']
                nDict['Longitude'] = geoL['longitude']
            js = json.dumps(nDict)
            producer.produce(js)
        except ValueError:
            pass
except KeyboardInterrupt:
    print "\nQuitting..."
    raise SystemExit
