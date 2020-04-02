package org.akshita.jain.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallBack {
    public static void main(String[] args) {

       final Logger logger = LoggerFactory.getLogger(ProducerDemoCallBack.class);
        System.out.println("Hello World 111111");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //create producer

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
       //create Producer Record
        for(int i = 0; i< 10; i++) {
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello World22" + Integer.toString(i));

            //send data ---asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time the record is successfully sent or an exception is thrown
                    if (e == null) {
                        //success
                        logger.info("Received new metadata." + "\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n" +
                                "Offset: " + recordMetadata.offset()
                        );
                    } else {
                        //error
                        logger.error("Error:", e);
                    }
                }
            });
        }
        //flush data
        producer.flush();
        //flush and close producer
        producer.close();


    }

}
