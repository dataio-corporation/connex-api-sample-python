Copyright 2023 Data I/O Corporation

# What is the ConneX API Sample Python?

The `connex-api-sample-python` repository hosts a collection of Python language sample code to interface with Data I/O's ConneX System. 
The purpose of these examples is to help developers to integrate solutions with ConneX's system using the Python programming language.

## Key Features

### MQTT Examples
- **ConneXMqttClient**: Basic MQTT client that connects to ConneX MQTT Broker and monitors the subscribed event messages.

### GraphQL Examples

TODO

## Usage

The following requirements must be met to run the examples in this repository:

1. ConneX 3.x installed, version 3.0.6 was used when testing.
1. Python 3.6+ installed, version 3.11.3 was used.
1. [paho.mqtt](https://pypi.org/project/paho-mqtt/) library installed, version 1.6.1 was used.
1. The computer to run the examples should be able to communicate with the ConneX system through TCP/IP.