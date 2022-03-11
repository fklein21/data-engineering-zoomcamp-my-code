from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv
from time import sleep


def load_avro_schema_from_file():
    key_schema = avro.load("taxi_ride_key.avsc")
    value_schema = avro.load("taxi_ride_value.avsc")

    return key_schema, value_schema


def send_record():
    key_schema, value_schema = load_avro_schema_from_file()

    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "acks": "1"
    }

    producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)

    file = open("data/rides.csv")

    csvreader = csv.reader(file)
    header = next(csvreader)
    for row in csvreader:
        key = {"vendorId": int(row[0])}
        value = {"vendorId": int(row[0])}