"""
ConneX MQTT Client sample code.

ConneXMqttClient [-h] [-i IPHOST] [-p PORT]

This script allows the user to connect to a ConneX MQTT Broker.

When successful, the script will log the subscribed event messages to 
both the console and a log file named 'ConneXMqttClient.log'.

This script requires that `paho.mqtt` be installed within the Python
environment you are running this script in.

To stop the script, simply press CTRL+C to abort the execution.
"""

import paho.mqtt.client as mqtt
import logging
import argparse
import datetime as dt
import random

# Helper class to use microseconds in logger timestamps
class uSecsFormatter(logging.Formatter):
    converter = dt.datetime.fromtimestamp
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s.%03d" % (t, record.msecs)
        return s

# Create a custom logger
logger = logging.getLogger(__name__)

# Create two log handlers, one for console output and one for file output
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler("ConneXMqttClient.log")
c_handler.setLevel(logging.INFO)
f_handler.setLevel(logging.INFO)

# Create formatters and add them to handlers
c_format = uSecsFormatter(fmt="[%(levelname)s] | %(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S.%f")
f_format = uSecsFormatter(fmt="[%(levelname)s] | %(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S.%f")
c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)

# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)

# Set logger threshold level
logger.setLevel(logging.INFO)

# Initialize argument parser
parser = argparse.ArgumentParser(usage=__doc__)

# Adding optional arguments
parser.add_argument("-i", "--iphost", help="ConneX MQTT Broker IP address or host name, default = localhost")
parser.add_argument("-p", "--port", type=int, help="ConneX MQTT Broker port, default = 1883")

# Subscribe to specified topic
def subscribe(client, topic):
    # Subscribe to topic
    client.subscribe(topic)    
    if topic == '#':
        logger.info("Subscribed to all event messages...")
    else:
        logger.info(f"Subscribed to '{topic}' event messages...")
    
# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info(f"Successfully connected to ConneX MQTT Broker!!!, client_id: {client._client_id.decode()}")
        
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        
        # Uncomment the example you want to test

        # Subscribe to light tower changed topic
        #subscribe(client, "ah700/lightowerchanged/#")

        # Subscribe to pick operations topic
        #subscribe(client, "ah700/operations/pick/#")

        # Subscribe to place operations topic
        #subscribe(client, "ah700/operations/place/#")

        # Subscribe to all topics
        subscribe(client, "#")
    else:
        logger.info(f"Failed to connect to ConneX MQTT Broker, result code: {rc}") 

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    logger.info(f"{msg.topic} | {msg.payload.decode()}")

# The callback for when a disconnect happens.
def on_disconnect(client, rc, properties):
    logger.info(f"Disconnected... client: {client._client_id.decode()}, return code: {rc}, properties: {properties}")

# Parse command line arguments
def parseArguments():
    # Set default return values
    host, port = "localhost", 1883

    # Read arguments from command line
    args = parser.parse_args()     
    if args.iphost:
        host = args.iphost
    if args.port:
        port = int(args.port)
    return (host, port)

# main program
def main():
    # Get host and port values to use for connecting to ConneX MQTT Broker
    host, port = parseArguments()
    
    # Add log start header, useful when several logs are appended to the same file
    logger.info("----------------------------------------------------------------------")
    logger.info("-------------------------- Starting new log --------------------------")
    logger.info("----------------------------------------------------------------------")

    # Initialize MQTT client, generate random id
    client = mqtt.Client(client_id=f'connex-mqtt-{random.randint(0, 1000)}')
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    # Use "localhost" if ConneX service is running in same machine
    # otherwise use IP address of machine where ConneX service is installed
    try:
        logger.info(f"Attempting connection to ConneX MQTT Broker... Host: {host}, Port: {port}")
        client.connect(host, port, 60)

        # Blocking call that processes network traffic, dispatches callbacks and
        # handles reconnecting.
        # Other loop*() functions are available that give a threaded interface and a
        # manual interface.
        client.loop_forever()
    except Exception as e:
        logger.exception("An exception occurred, could not connect to ConneX MQTT Broker...") 
        input("Press any key to continue...")

# Script entry point
if __name__ == '__main__':
    main()