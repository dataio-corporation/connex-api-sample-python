"""
ConneX MQTT Command sample code.

ConneXMqttCmd [-h] [-i IPHOST] [-p PORT]

This script allows the user to connect to a ConneX MQTT Broker and
issue commands to a machine manager to launch DMS or TaskLink. 
It can also issue commands to an automated handler to pause and/or 
abort a running job.

Once the script is running, use the following keys to perform an action:

Press 'd' to publish machine manager command to launch DMS
Press 't' to publish machine manager command to launch TaskLink
Press 'p' to publish automated handler 'pause' command
Press 'a' to publish automated handler 'abort' command
Press 'q' to exit script

This script requires the following libraries to be installed within 
the Python environment: `paho.mqtt` and `keyboard`.

Alternatively, we can also press CTRL+C to abort the execution.
"""

import paho.mqtt.client as mqtt
import logging
import argparse
import datetime as dt
import random
import time
import keyboard

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
f_handler = logging.FileHandler("ConneXMqttCmd.log")
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

# Initialize auxiliary global variables
connected = False
sessionid = "Undefined"
keep_running = True

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
    global connected
    if rc == 0:
        logger.info(f"Successfully connected to ConneX MQTT Broker!!!, client_id: {client._client_id.decode()}")
        
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.

        # Subscribe to all topics
        subscribe(client, "#")
        
        connected = True
    else:
        logger.info(f"Failed to connect to ConneX MQTT Broker, result code: {rc}") 

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    global sessionid
    logger.info(f"{msg.topic} | {msg.payload.decode()}")
    # When the topic contains 'startup', update sessionid
    if "startup" in msg.topic:
        sessionid = msg.topic.split("/")[3]
        logger.info(f"==========> Updated sessionid: {sessionid}")
        

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

# Publish command to MQTT broker
def publish(client, topic, payload):
    result = client.publish(topic, payload)
    if result.rc == 0:
        logger.info(f"Published topic '{topic}' with payload '{payload}'")
    else:
        logger.info(f"Failed to publish topic '{topic}'")
        
# The callback for when the user presses a key to send command to machine manager
def on_machine_manager_command(client, topic, payload):    
    logger.info(f"==========> User requested to publish machine manager command!")
    publish(client, topic, payload)

# The callback for when the user presses a key to send command to automated handler
def on_handler_command(client, topic, payload):
    global sessionid
    if sessionid == "Undefined":
        logger.info(f"==========> Unable to publish automated handler command!, sessionid is not defined yet.")
    else:
        logger.info(f"==========> User requested to publish automated handler command!")
        publish(client, f"{topic}/{sessionid}", payload)

# The callback for when the user presses 'q' to stop the script
def on_quit():
    global keep_running
    logger.info("Stopping script...")
    keep_running = False

# main program
def main():
    global connected
    global keep_running
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

    # Define shortcut keys to process machine manager commands
    keyboard.add_hotkey('d', on_machine_manager_command, 
                        args=[client, "command/dms/launchdms/dell004", 
                              "{\"JobName\":\"Verify Memory (2GB)\",\"Quantity\":10}"])
    keyboard.add_hotkey('t', on_machine_manager_command, 
                        args=[client, "command/tasklink/launchtasklink/dell004", 
                              "{\"TaskName\":\"TEST\",\"AdministratorMode\":true,\"BatchMode\":true,\"Quantity\":10}"])

    # Define shortcut keys to process automated handler commands
    keyboard.add_hotkey('p', on_handler_command, args=[client, "command/ah700/pausejob/dell004", "{}"])
    keyboard.add_hotkey('a', on_handler_command, args=[client, "command/ah700/abortjob/dell004", "{}"])

    # Define shortcut key to stop script
    keyboard.add_hotkey('q', on_quit)

    # Use "localhost" if ConneX service is running in same machine
    # otherwise use IP address of machine where ConneX service is installed
    try:
        logger.info(f"Attempting connection to ConneX MQTT Broker... Host: {host}, Port: {port}")
        client.connect(host, port, 60)

        # Start to process messages
        client.loop_start()

        # Wait until on_connect has happened
        while not connected:
            logger.info("Waiting for connection...")
            time.sleep(1)
        
        # Loop until user presses 'q' to exit script
        # User can press 'd' to publish publish machine manager command to launch DMS
        # User can press 't' to publish machine manager command to launch TaskLink
        # User can press 'p' to publish automated handler 'pause' command
        # User can press 'a' to publish automated handler 'abort' command
        while keep_running:
            time.sleep(1)

        # Stop message processing
        client.loop_stop()
    except Exception as e:
        logger.exception("An exception occurred, could not connect to ConneX MQTT Broker...") 
        input("Press any key to continue...")

# Script entry point
if __name__ == '__main__':
    main()
