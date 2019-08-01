package com.nandakumar111.kafka.sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) throws Exception{
//        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
//        String bootstrapServers = "localhost:9092";
//        String groupId = "check1_app_group4";
//        String topic = "check1";
//
//        // create consumer config
//        Properties properties = new Properties();
//        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//        // create consumer
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
//
//        // subscribe consumer to our topic(s)
//        consumer.subscribe(Arrays.asList(topic));
//
//        //  poll for new data
//        while(true){
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//
//            for(ConsumerRecord<String, String> record : records){
//                logger.info("Key : " + record.key() + ", Value : " + record.value());
//                logger.info("Partitions : " + record.partition() + ", Offset : " + record.offset());
//                logger.info("Topic : " + record.topic());
//            }
//        }
        ConsumerDemoWithThread.run();
    }
    private ConsumerDemoWithThread(){

    }
    private static void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
        String bootstrapServers = "localhost:9092";
        String groupId = "check1_app_group4";
        String topic = "check1";

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create the consumer runnable
        logger.info("Creating the consumer thread");
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers,groupId,topic, latch );

        // start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(
                ()->{
                    logger.info("Caught shutdown hook");
                    myConsumerRunnable.shutDown();
                }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted",e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public static class ConsumerRunnable implements Runnable{
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        private KafkaConsumer<String, String> consumer;
        private CountDownLatch latch;

        private ConsumerRunnable(String bootstrapServers,
                                String groupId,
                                String topic,
                                CountDownLatch latch){
            this.latch = latch;

            // create consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create consumer
            this.consumer = new KafkaConsumer<String, String>(properties);

            // subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));
        }
        @Override
        public void run() {
            try {
                //  poll for new data
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key : " + record.key() + ", Value : " + record.value());
                        logger.info("Partitions : " + record.partition() + ", Offset : " + record.offset());
                        logger.info("Topic : " + record.topic());
                    }
                }
            } catch (WakeupException e){
                logger.info("Receive shutdown signal!");
            } finally {
                consumer.close();
                // done with consumer
                latch.countDown();
            }
        }

        public void shutDown(){
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw WakeUpException
            consumer.wakeup();
        }
    }
}
