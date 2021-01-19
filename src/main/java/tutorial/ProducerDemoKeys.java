package tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("Hello World!");

        final Logger logger= LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        //Producer Config
        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Producer create

        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);

        //create a producer record

        for(int i=0;i<10;i++)
        {
            String topic_name="first_topic";
            String value="Kafka_Demo"+i;
            String key="id_"+i;
            ProducerRecord<String,String> record=new ProducerRecord<String, String>(topic_name,key,value);
            //send data --async

            logger.info("key: "+key);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an exception
                    if(e==null)
                    {
                        logger.info("Received new metadata \n"+
                                "Topic "+ recordMetadata.topic()+
                                "\n Partition "+recordMetadata.partition()
                                +"\n Offset "+recordMetadata.offset()
                                +"\n TimeStamp "+recordMetadata.timestamp());
                    }
                    else
                    {
                        logger.error("Error while Producing "+e);
                    }
                }
            }).get();// dont do in production

        }

        producer.flush();
        producer.close();
    }
}
