# About Project:
  - Pull the realtime rsvp data from meetup api through web socket.
  - Using Kafka producer, send the data as messages to Kafka consumer.
  - Using Kafka consumer, receive the messages from Kafka Producer.
  - Store the data received through Kafka consumer into Cassandra database.
  - Start an Apache SparkContext, read the data from Cassandra database.
  - Perform Analysis using Apache Spark.
 
# Tools Used
 - Programming:
  - Python
  - Scala
 - Big Data:
  - Apache Kafka
  - Apache Spark
 - Database:
  - Cassandra

# How to run(sequence)
 - Start Zookeeper
 - Start Kafka
 - Start Cassandra
 - Run rsvp_producer.py
 - Run rsvp_consumer.py
 - Run spark_analysis/src/main/scala/SparkAnalysis.scala