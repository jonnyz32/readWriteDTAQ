package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerExample {

    public static void main(String[] args) {
        // Kafka configuration properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");  // Kafka broker address (local)
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");  // Ensure all replicas acknowledge the message

        // Create KafkaProducer instance
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Kafka topic to send messages to
        String topic = "my_topic";  // Change to your actual topic name

        // Send 1000 messages to Kafka topic
        for (int i = 1; i <= 100000; i++) {
            long timestampSeconds = System.currentTimeMillis() / 1000;
            String message = "Message " + i;
            // String msg = String.valueOf(timestampSeconds) + Integer.toString(i);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, String.valueOf(timestampSeconds), message);

            // Send the message asynchronously
            producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                if (exception != null) {
                    exception.printStackTrace();  // Log the error if any
                } else {
                    System.out.println("Sent message: " + message + " to topic: " + topic);
                }
            });
        }

        // Close the producer after sending messages
        producer.close();
    }
}
