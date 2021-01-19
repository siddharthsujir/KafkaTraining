package tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ComsumerDemo {

    public static void main(String[] args) {
        System.out.println("Comsumer Code!");
        Logger logger= LoggerFactory.getLogger(ComsumerDemo.class.getName());

        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"my-consumer-application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(properties);

        //subscribe Consumer to our Topic

        consumer.subscribe(Collections.singleton("first_topic"));

        //poll for data

        while (true)
        {
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record:records)
            {
             logger.info("key "+record.key()+" value "+ record.value() +" offset "+record.offset());
            }
        }
    }
}
