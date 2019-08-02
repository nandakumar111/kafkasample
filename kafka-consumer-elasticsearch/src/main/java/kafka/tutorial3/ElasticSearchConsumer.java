package kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
    private static RestHighLevelClient createClient(){
        // ES credentials
        String hostname ="learning-kafka-4900731908.ap-southeast-2.bonsaisearch.net";
        String username ="gfbamiy83h";
        String password ="r6elv0scv5";

        // if it's not local ES
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });

        return new RestHighLevelClient(builder);
    }

    private static KafkaConsumer<String, String> createConsumer(String topic){
        String bootstrapServers = "localhost:9092";
        String groupId = "kafka-demo-elasticsearch";

        // create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable autocommit offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        BulkRequest bulkRequest = new BulkRequest();

        //  poll for new data
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            int recordsCount = records.count();
            logger.info("Received " + recordsCount + " record(s).");
            for(ConsumerRecord<String, String> record : records){

                // to avoid duplication adding unique id
                // kafka generic id
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                try{
                    //twitter feed specific ID
                    String id = extractIdFromTweet(record.value());

                    IndexRequest request = new IndexRequest(
                            "twitter",
                            "tweets",
                            id                  // this is to make our consumer idempotent
                    ).source(record.value(), XContentType.JSON);

                    /*
                     *  ES Error : Limit of total fields [1000] in index has been exceeded
                     *
                     *  TYPE : PUT
                     *  URL  : <elasticsearch_host>:<port>/<index_name>/_settings
                     *  DATA : { "index.mapping.total_fields.limit": 10000 }
                     *
                     * */

                    bulkRequest.add(request); // bulk request (takes no time)
                } catch (NullPointerException e){
                    logger.warn("skipping bad data ::: " + record.value());
                }
            }
            if (recordsCount > 0){
                BulkResponse responses = client.bulk(bulkRequest,RequestOptions.DEFAULT);
                logger.info("committing offsets...");
                consumer.commitSync();
                logger.info("offset have been committed");
                Thread.sleep(1000);
            }
        }

        // close the client
//        client.close();
    }

    private static String extractIdFromTweet(String tweetJson){
        return new JsonParser()
                .parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
