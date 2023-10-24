package org.kafka.demo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.logging.Logger;

public class ProducerDemoWithKey {
    private static final Logger log = Logger.getLogger(ProducerDemoWithKey.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer with Key!");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //se producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a producer record
        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {

                String topic = "demo_java";
                String key = "id_" + i;
                String value = "I am a Kafka Producer with key " + i;

                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                //send data with Callback
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // excuted every time a record is successfully sent or an exception is thrown
                        if (exception == null) {
                            //the record was successfully sent
                            log.info(
                                    "Key: " + key + " | Partition: " + metadata.partition() + "\n");
                        } else {
                            log.warning("Error while producing: " + exception);
                        }

                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }

        //tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //flush and close producer
        producer.close();

        /**
         * starting zookeeper and kafka server from command line (in separate terminals)
         * zookeeper-server-start.sh ~/kafka_2.13-3.6.0/config/zookeeper.properties
         * kafka-server-start.sh ~/kafka_2.13-3.6.0/config/server.properties
         * run the consumer
         * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo_java --from-beginning
         */
    }
}
