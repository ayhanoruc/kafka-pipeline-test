from src.kafka_consumer.json_consumer   import consumer_using_sample_file
#from src.constant import SAMPLE_DIR
import os 




if __name__ == '__main__':
    topics = os.listdir("C:/Users/ayhan/Desktop/kafka-pipeline-test/sample_data")
    print(f"topics: [{topics}]")


    for topic in topics:
        #sample_topic_data_dir = os.path.join(SAMPLE_DIR,topic)
        #sample_file_path = os.path.join(sample_topic_data_dir,os.listdir(sample_topic_data_dir)[0])
        sample_file_path = os.path.join(".", "sample_data", "kafka-sensor-topic", "aps_failure_training_set_processed_8bit.csv") # hardcoded
        consumer_using_sample_file(topic=topic,file_path=sample_file_path)