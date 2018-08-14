from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka.exceptions import NoBrokersAvailableError, UnknownTopicOrPartition
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import *
from cryptography.hazmat.primitives.serialization import *
#from cryptography.hazmat.primitives.asymmetric.rsa import *
import cryptography.hazmat.primitives.asymmetric
import cryptography
import json
import threading
import logging
import time
import sys
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

def Extension(Certificate):
    ext = cert.extensions
    for extensions in ext:
        nDict['Extension'] = str(extensions.value)
        print extensions.value
def Nvb(Certificate):
    nvb = cert.not_valid_before
    nDict['NotValidBefore'] = str(nvb)

def Nva(Certificate):
    nva = cert.not_valid_after
    nDict['NotValidAfter'] = str(nva)

def Subject(Certificate):
    subject = cert.subject
    sDict = {}
    for attribute in subject:
        sDict[str(attribute.oid._name)] = str(attribute.value)
    nDict['Subject'] = sDict

def Version(Certificate):
    try:
        version = cert.version
        nDict['version'] = str(version)
    except cryptography.x509.InvalidVersion:
        pass

def SerialNumber(Certificate):
    serialNumber = cert.serial_number
    nDict['SerialNumber'] = str(serialNumber)

#def PublicKey(Certificate):
#    publicKey = cert.public_key()
#    print dir(publicKey.public_bytes(PEM, SubjectPublicKeyInfo))#.public_bytes(PEM, SubjectPublicKeyInfo))

def Issuer(Certificate):
    issuer = cert.issuer
    for attribute in issuer:
        nDict[str(attribute.oid._name)] = str(attribute.value)

def SignatureAlg(Certificate):
    sigAlg = cert.signature_algorithm_oid
    nDict['SignatureAlgorithm'] = str(sigAlg._name)

def MakeJson(Dict):
    js = json.dumps(Dict)
    producer.produce(js)

try:
    for message in consumer:
        try:
            servHi = False
            cliHi = False
            nDict = json.loads(message.value) #loads current json to dict
            try:
                tlsDict = nDict['tls']
                    #if 'msgTypes' in tlsDict:
                msgDict = tlsDict['msgTypes']
                        #if 'Certificate' in msgDict:
                if 'ServerHello' in msgDict:
                        servHi = True
                        nDict['ServerHello'] = servHi
                if 'ClientHello' in msgDict:
                        cliHi = True
                        nDict['ClientHello'] = cliHi
                certList = tlsDict['certificates']
                for attribute in certList:
                    aCert = attribute
                    bCert = str(aCert[2:]).decode('hex').encode('base64')
                    cCert ="-----BEGIN CERTIFICATE-----\n" + bCert + "-----END CERTIFICATE-----\n"
                    cert = x509.load_pem_x509_certificate(cCert, default_backend())

                    #Extension(cert)
                    #Subject(cert)
                    #Nvb(cert)
                    #Nva(cert)
                    #Version(cert)
                    #SerialNumber(cert)
                    #Issuer(cert)
                    #SignatureAlg(cert)

                    #print cert.fingerprint(hashes.SHA256())
                    #print cert.signature_algorithm_oid
                    #Add error handling for this
                    #print dir(cert.signature)
                    #print cert.signature.translate
                    #print cert.tbs_certificate_bytes
                    #Need to add cert data to json here for cert-packets
                #Need to make sure non-cert packages are still going through.
                MakeJson(nDict)
            except KeyError:
                pass
        except ValueError:
            pass
except KeyboardInterrupt:
    try:
        print "\nQuitting..."
        raise SystemExit
    except TypeError or AttributeError:
        raise SystemExit
