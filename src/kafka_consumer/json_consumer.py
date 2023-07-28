import sys
sys.path.append(r"C:\Users\ayhan\Desktop\kafka-pipeline-test\src")

import argparse
from confluent_kafka import Consumer 
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from entity.generic import Generic 
from kafka_config import sasl_conf
from database.mongodb import MongodbOperation






def consumer_using_sample_file(topic, file_path):

    schema_str = Generic.get_schema_to_produce_consume_data(file_path=file_path)
    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Generic.dict_to_object
                                         )
    consumer_conf = sasl_conf()
    consumer_conf.update({
        'group.id' : 'group1',
        'auto.offset.reset' : 'earliest'
    })

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    mongodb= MongodbOperation()
    records= []

    x=0
    while True:
        try: 
            msg= consumer.poll(1.0)
            if msg is None:
                continue 
            
            record: Generic = json_deserializer(msg.value(),SerializationContext(msg.topic(),MessageField.VALUE))

            if record is not None:
                records.append(record.to_dict())
                if x%500 ==0:
                    mongodb.insert_many(collection_name="kafka-sensor-data",records=records)
                    records = []
            x+=1
            print(x)

        except KeyboardInterrupt:
            break 


    consumer.close()