package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getName());

    public static void main(String[] args) {

        String groupId = "my-java-application";

        log.info("\nConsumer Started....");

        //Consumer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id",groupId);
        properties.setProperty("auto.offset.reset","earliest");


        // Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //Subscribe to Topic
        consumer.subscribe(Arrays.asList("demo_java"));

        // poll for data
        // Infinite loop
        while (true){
            log.info("\nPolling....");
            // It will fetch list of records
           ConsumerRecords<String,String> records =  consumer.poll(Duration.ofMillis(1000));
           for (ConsumerRecord<String, String> record : records){
               log.info("\n =================================================");
               log.info("\nKey :::"+record.key()+" Value :::"+record.value());
               log.info("\nPartition :::"+record.partition()+" Offset :::"+record.offset());
               log.info("\n =================================================");
           }
        }
    }
}
