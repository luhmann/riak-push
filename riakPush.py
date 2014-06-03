import argparse
import os
import requests
import json
from riak import RiakClient, RiakNode

parser = argparse.ArgumentParser(description='Takes a list of urls and ')
parser.add_argument('--srcRiak', dest='srcRiak', default='10.228.39.181', help='The host we get the redis data from')
parser.add_argument('--destRiak', dest='destRiak', default='int-riak-01.magic-technik.de', help='The host we get the redis data from')
args = parser.parse_args()

# map arguments
srcRiakHost = args.srcRiak
destRiakHost = args.destRiak

baseDir = os.path.dirname(os.path.realpath(__file__))

# Parses the keys.txt file and writes url
def readRiakKeys():
    keyListFilename = os.path.join(baseDir, 'keys.txt')
    return  [line.strip() for line in open(keyListFilename, 'r')]


# unused helper method to save an image to the disk if you want that
def saveImage(riakObj, filename):
    with open(os.path.join(baseDir, filename), 'w+') as f:
        f.write(riakObj.encoded_data)


# Read in redis-keys
imgs = readRiakKeys()
print 'Riak Keys: \n' + '\n'.join(imgs)

# connect to live riak
rcLive = RiakClient(protocol='http', host=srcRiakHost, http_port=8098)
rbLive = rcLive.bucket('ez')

# connect to integration riak
rcInt = RiakClient(protocol='http', host=destRiakHost, http_port=8098)
rbInt = rcInt.bucket('ez')

# save image in integration riak
def saveToIntRiak(key, riakObj):
    if riakObj.content_type is not None:
        img = rbInt.new(key, encoded_data=riakObj.encoded_data, content_type=riakObj.content_type)
        img.store()

# get and save all images
for img in imgs:
    print img
    local = rbInt.get(img)
    print local.exists
    if local.exists is False:
        buffer = rbLive.get(img)
        saveToIntRiak(img, buffer)
