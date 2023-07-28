import sys
sys.path.append(r"C:\Users\ayhan\Desktop\kafka-pipeline-test\src")

import argparse
from uuid import uuid4 

from six.moves import input 
from kafka_config import sasl_conf, schema_config
#from src.kafka_config import sasl_conf, schema_config
from logger import logger
from confluent_kafka import Producer 
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import pandas as pd 
from typing import List 
from entity.generic import Generic,instance_to_dict



def delivery_report(err,msg):
    if err is not None: 
        logger.info("Delivery failed for user record {}:{}".format(msg.key(),err))
        return

    logger.info('Sensor record {} succesfully produced to {} [{}] at offset {}'.format(
        msg.key(),msg.topic(), msg.partition(),msg.offset()
    ))




def product_data_using_file(topic,file_path):
    schema_str = Generic.get_schema_to_produce_consume_data(file_path=file_path)
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)


    string_serializer = StringSerializer('utf-8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client,instance_to_dict)

    producer = Producer(sasl_conf())

    print("Producing user records to topic{}. ^C to exit.".format(topic))


    producer.poll(0.0)
    i=0
    try:
        for instance in Generic.get_object(file_path=file_path):
            
            producer.produce(topic=topic,
                             key=string_serializer(str(uuid4()),instance.to_dict()),
                             value= json_serializer(instance,SerializationContext(topic,MessageField.VALUE)),
                             on_delivery= delivery_report                             
                             )
            i+=1
            print(i)
    except KeyboardInterrupt:
        pass 

    except ValueError:
        print("Invalid input, discarding record...")
        pass 

    print("\nFlushing records...")
    producer.flush()


print("finished")



