#!/usr/bin/env python

# # pyspark Library
import json
import csv
import datetime

# Kakfa Library
from kafka import KafkaConsumer

kafka_brokers_list = ["localhost:9092"] # We can put multiple brokers here.
group_id = "meetup_rsvp_stream" 

# Kafka Consumer
consumer = KafkaConsumer("rsvp_stream", group_id=group_id,
                        metadata_broker_list = kafka_brokers_list)


from cqlengine import columns
from cqlengine.models import Model
# Managing schemas
from cqlengine.management import sync_table, create_keyspace



# Model Definition
class Rsvpstream(Model):
    venue_name = columns.Text()
    venue_lon = columns.Decimal(required=False)
    venue_lat = columns.Decimal(required=False)
    venue_id = columns.Integer()
    visibility = columns.Text()
    response = columns.Text()
    guests = columns.Integer()
    member_id = columns.Integer()
    member_name = columns.Text()
    rsvp_id = columns.Integer(primary_key=True)
    rsvp_last_modified_time = columns.DateTime(required=False)
    event_name = columns.Text()
    event_time = columns.DateTime(required=False)
    event_url = columns.Text()
    group_topic_names = columns.Text()
    group_country = columns.Text()
    group_state = columns.Text()
    group_city = columns.Text()
    group_name = columns.Text()
    group_lon = columns.Integer()
    group_lat = columns.Integer()
    group_id = columns.Integer()


# Setup connection
from cqlengine import connection
# Connect to the meetup keyspace on our cluster running at 127.0.0.1
connection.setup(['127.0.0.1'], "meetup")
# create keyspace
# keyspace name, keyspace replication strategy, replication factor
create_keyspace('meetup', 'SimpleStrategy', 1)
# Sync your model with your cql table
sync_table(Rsvpstream)

if consumer:
    for message in consumer:
        message = json.loads(message.value) #Convert to json
        venue = message.get('venue')
        if venue:
            venue_name = venue.get('venue_name')
            venue_lon = venue.get('lon')
            venue_lat = venue.get('lat')
            venue_id = venue.get('venue_id')
        else:
            venue_name, venue_lon, venue_lat, venue_id = None, None, None, None
        visibility = message.get('visibility')
        response = message.get('response')
        guests = message.get('guests')
        # Member who RSVP'd
        member = message.get('member')
        if member:
            member_id = member.get('member_id')
            member_name = member.get('member_name')
        else:
            member_id, member_name = '', ''
        rsvp_id = message.get('rsvp_id')
        #since epoch
        # rsvp_last_modified_time = datetime.datetime.fromtimestamp(float(message.get('mtime'))/1000.0).strftime('%Y-%m-%d %H:%M:%S')
        mtime = message.get('mtime')
        if mtime:
            rsvp_last_modified_time = datetime.datetime.utcfromtimestamp(float(mtime)/1000.0)
        else:
            rsvp_last_modified_time = None
        # Event for the RSVP
        event = message.get('event')
        if event:
            event_name = event.get('event_name')
            # event_time = datetime.datetime.fromtimestamp(float(event.get('time'))/1000.0).strftime('%Y-%m-%d %H:%M:%S')
            time = event.get('time')
            if time:
                event_time = datetime.datetime.utcfromtimestamp(float(time)/1000.0)
            else:
                event_time = None
            event_url = event.get('event_url')
        else:
            event_name, event_id, event_time, event_url = '', '', '', ''
        # Group hosting the event
        group = message.get('group')
        if group:
            group_topics = group.get('group_topics')
            if group_topics:
                group_topic_names = ','.join([each_group_topic.get('topic_name') for each_group_topic in group_topics])
                type(group_topic_names)
            else:
                group_topic_names = ''
            group_city = group.get('group_city')
            group_country = group.get('group_country')
            group_id = group.get('group_id')
            group_name = group.get('group_name')
            group_lon = group.get('group_lon')
            group_state = group.get('group_state')
            group_lat = group.get('group_lat')
        else:
            group_topic_names, group_city, group_country, group_id, group_name, group_lon, \
            group_state, group_lat = '', '', '', '', '', '', '', ''

        try:
            Rsvpstream.create(venue_name = venue_name, venue_lon = venue_lon, venue_lat = venue_lat, \
                venue_id = venue_id, visibility = visibility, response = response, guests = guests, \
                member_id = member_id, member_name = member_name, rsvp_id = rsvp_id, \
                rsvp_last_modified_time  = rsvp_last_modified_time, \
                event_name = event_name, event_time = event_time, event_url = event_url, \
                group_topic_names = group_topic_names, group_country = group_country, \
                group_state = group_state, group_city = group_city, group_name = group_name, \
                group_lon = group_lon, group_lat = group_lat, group_id = group_id
                )
        except Exception, e:
            print e

kafka.close()