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
    geo = GeoIP.open("GeoLiteCity.dat", GeoIP.GEOIP_STANDARD)
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
            ipDict = nDict["ipv4"] #Loads the keys inside ipv4 into a new dict
            geoL = geo.record_by_addr(ipDict["srcAddr"]) #Get Lat/Long from ip
            geoLd = geo.record_by_addr(ipDict["dstAddr"])
            sDict = {}
            dDict = {}
            if geoL is not None:
                sDict['Latitude'] = geoL['latitude']
                sDict['Longitude'] = geoL['longitude']
                sDict['City'] = geoL['city']
                sDict['RegionName'] = geoL['region_name']
                sDict['Region'] = geoL['region']
                sDict['areaCode'] = geoL['area_code']
                sDict['TimeZone'] = geoL['time_zone']
                sDict['MetroCode'] = geoL['metro_code']
                sDict['CountryCode3'] = geoL['country_code3']
                sDict['PostalCode'] = geoL['postal_code']
                sDict['dmaCode'] = geoL['dma_code']
                sDict['CountryCode'] = geoL['country_code']
                sDict['CountryName'] = geoL['country_name']
                nDict['src'] = sDict
            if geoLd is not None:
                dDict['Latitude'] = geoLd['latitude']
                dDict['Longitude'] = geoLd['longitude']
                dDict['City'] = geoLd['city']
                dDict['RegionName'] = geoLd['region_name']
                dDict['Region'] = geoLd['region']
                dDict['areaCode'] = geoLd['area_code']
                dDict['TimeZone'] = geoLd['time_zone']
                dDict['MetroCode'] = geoLd['metro_code']
                dDict['CountryCode3'] = geoLd['country_code3']
                dDict['PostalCode'] = geoLd['postal_code']
                dDict['dmaCode'] = geoLd['dma_code']
                dDict['CountryCode'] = geoLd['country_code']
                dDict['CountryName'] = geoLd['country_name']
                nDict['dst'] = dDict
            produceJSON(nDict)
        except ValueError:
            pass
except KeyboardInterrupt:
    print "\nQuitting..."
    raise SystemExit
