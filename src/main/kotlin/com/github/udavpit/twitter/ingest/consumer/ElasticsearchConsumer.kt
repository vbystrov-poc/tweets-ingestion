package com.github.udavpit.twitter.ingest.consumer

import com.google.gson.JsonParser
import org.apache.http.HttpHost
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory
import java.time.Duration

/**
 * Elasticsearch consumer. Consumes tweets from Kafka topic and store them to Elasticsearch.
 *
 * @author Vladimir Bystrov
 */
class ElasticsearchConsumer {

    private val logger = LoggerFactory.getLogger(ElasticsearchConsumer::class.java)

    /**
     * Creates an Elasticsearch REST client.
     *
     * @return a RestHighLevelClient instance
     */
    private fun createElasticClient(): RestHighLevelClient {
        val hostname = "localhost"
        val builder = RestClient.builder(HttpHost(hostname, 9200, "http"))

        return RestHighLevelClient(builder)
    }

    /**
     * Creates a Kafka consumer that reads tweets from Kafka and save them to ElasticSearch.
     *
     * @return a KafkaConsumer<String, String> instance
     */
    private fun createKafkaConsumer(): KafkaConsumer<String, String> {
        val bootstrapServers = "127.0.0.1:9092"
        val groupId = "kafka-twitter-streaming-grp"

        val props = mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                ConsumerConfig.GROUP_ID_CONFIG to groupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false", // disable autocommit offsets
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG to "20" // max records to poll
        ).toProperties()

        return KafkaConsumer(props)
    }

    /**
     * Extracts an ID from tweet
     *
     * @param data
     */
    private fun extractIdFromTweet(data: String): String {
        // using gson to parse JSON
        return JsonParser.parseString(data).asJsonObject.get("id_str").asString
    }

    /**
     * Consumes tweets from Kafka topic and store them to Elasticsearch.
     */
    fun run() {
        val client = createElasticClient()

        val consumer = createKafkaConsumer()
        consumer.subscribe(listOf("kafka_tweets"))

        Runtime.getRuntime().addShutdownHook(Thread {
            logger.info("Stopping Application...")
            client.close()
            consumer.close()
        })

        while (true) {
            val records = consumer.poll(Duration.ofMillis(100))

            logger.info("Received ${records.count()} records")

            for (record in records) {
                // 2 strategies to manage IDs

                // Kafka generic ID
                // val id: String = "${record.topic()}_${record.partition()}_${record.offset()}"

                // twitter feed specific ID
                val id = extractIdFromTweet(record.value())

                val indexRequest = IndexRequest("twitter_tweets")
                        .id(id)
                        .source(record.value(), XContentType.JSON)

                val indexResponse = try {
                    client.index(indexRequest, RequestOptions.DEFAULT)
                } catch (ex: Exception) {
                    ex.printStackTrace()
                    null
                }

                logger.info(indexResponse?.id)

                try {
                    Thread.sleep(10)
                } catch (ex: InterruptedException) {
                    ex.printStackTrace()
                }
            }

            logger.info("Commit the offset...")

            consumer.commitSync()

            logger.info("Offset committed")

            try {
                Thread.sleep(1000)
            } catch (ex: InterruptedException) {
                ex.printStackTrace()
            }
        }
    }
}

/**
 * Run the consumer
 */
fun main() {
    ElasticsearchConsumer().run()
}