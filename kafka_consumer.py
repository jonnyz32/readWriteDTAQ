from confluent_kafka import Consumer, KafkaException, KafkaError, Producer
import time

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        
def ask_model(recipient, question):
    time.sleep(0.15)
    return question
        
def process_message(msg):
    answer = ask_model(msg)
    return answer

def sendBatch(batch):
    recipient = get_recipient()
    ret = ask_model(recipient, batch)
    return ret

def get_recipient():
    pass
    
    
    

# Kafka Consumer configuration
conf = {
    'bootstrap.servers': '9.5.12.241:9092',  # Replace with your Kafka server address
    'group.id': 'my-consumer-group',  # Consumer group ID
    'auto.offset.reset': 'earliest',  # Start reading from the earliest message
}

# Create Consumer instance
consumer = Consumer(conf)
producer = Producer(conf)

# Kafka topic to consume messages from
topic = 'my_topic'  # Replace with your topic name
topic_2 = 'recv_topic'

# Subscribe to the topic
consumer.subscribe([topic])

# Start consuming messages
try:
    start = time.process_time()
    batch = []
    while True:
        # Poll for a message (this will block until a message is received or timeout occurs)
        msg = consumer.poll(timeout=1.0)  # Adjust timeout as necessary

        if msg is None:
            # No message received within the timeout
            continue
        if msg.error():
            # Handle errors (if any)
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"End of partition reached {msg.partition}, offset {msg.offset()}")
            else:
                raise KafkaException(msg.error())
        else:
            # Process the message
            decoded_val = msg.value().decode('utf-8')
            decoded_key = msg.key().decode('utf-8')
            if (len(batch) < 500 and time.process_time() - start < 5):
                batch.append([decoded_key, decoded_val])
            else:
                res = sendBatch(batch)
                for record in res:
                    producer.produce(topic_2, key=record[0], value=record[1], callback=delivery_report)
                print("last record sent", batch[-1][0], batch[-1][1])
                producer.poll(0)
                batch = []
                start = time.process_time()

            # answer = process_message(decoded_val)
            # print(f"Received key: {decoded_key}")
            # print(f"Received val: {decoded_val}")
            

            # producer.produce(topic_2, key=decoded_key, value=answer, callback=delivery_report)
            # producer.poll(0)

            

except KeyboardInterrupt:
    print("Consumer interrupted")

finally:
    # Close the consumer when done
    consumer.close()
