package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithAssignSeek {
    public static void main(String[] args) throws Exception{
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithAssignSeek.class.getName());
        String bootstrapServers = "localhost:9092";
        String topic = "check1";

        // create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // assign
        TopicPartition partitionToReadsFrom = new TopicPartition(topic, 0);
        long offsetToReadsFrom = 5L;
        consumer.assign(Arrays.asList(partitionToReadsFrom));

        // seek
        consumer.seek(partitionToReadsFrom, offsetToReadsFrom);

        int numberOfMessagesToRead = 5;
        boolean keepReading = true;
        int numberOfMessagesReadSoFar = 0;

        //  poll for new data
        while(keepReading){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : records){
                numberOfMessagesReadSoFar++;
                logger.info("Key : " + record.key() + ", Value : " + record.value());
                logger.info("Partitions : " + record.partition() + ", Offset : " + record.offset());
                logger.info("Topic : " + record.topic());
                if(numberOfMessagesToRead <= numberOfMessagesReadSoFar){
                    keepReading = false;
                    break;
                }
            }
        }
        logger.info("Existing the application");
    }
}
