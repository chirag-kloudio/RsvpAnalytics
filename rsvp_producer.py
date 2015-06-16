#!/usr/bin/env python

# Standard Libraries
import json

# Kafka Library
from kafka import SimpleProducer, KafkaClient

# https://github.com/liris/websocket-client
from websocket import create_connection

# Kafka Producer
kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)

# To keep track of how many tweets we got.
count = 0

def rsvp_source():
    global count
    ws = create_connection('ws://stream.meetup.com/2/rsvps')
    while True:
        try:
            rsvp_data = ws.recv() # Get realtime data using sockets.
            if rsvp_data:
                producer.send_messages("rsvp_stream", rsvp_data)
        except:
            rsvp_source()

if __name__ == '__main__':
    rsvp_source()
