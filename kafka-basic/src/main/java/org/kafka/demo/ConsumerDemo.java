package org.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static String groupId = "my-fourth-application";
    private static final String topic = "demo_java";
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        System.out.println("I am a Kafka Consumer!");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

         /* the value of the offset can be earliest/latest/none
        earliest: read from the beginning of the topic
        latest: read only the new messages
        none: throw an error if there is no offset being saved
         */
        properties.setProperty("auto.offset.reset", "earliest" );



        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data
        while (true) {
            log.info("Polling");
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> consumerRecord : records) {
                log.info("Key: " + consumerRecord.key() + " | Value: " + consumerRecord.value());
                log.info("Partition: " + consumerRecord.partition() + " | Offset: " + consumerRecord.offset());
            }
        }



    }
}
