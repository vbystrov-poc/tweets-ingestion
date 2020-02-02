package com.github.udavpit.twitter.ingest.producer

import com.github.udavpit.twitter.ingest.config.TwitterConfig
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Client
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.Hosts
import com.twitter.hbc.core.HttpHosts
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.Authentication
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/**
 * Tweets producer. Reads tweets from Twitter and sends them to Kafka topic.
 *
 * @author Vladimir Bystrov
 */
class TwitterProducer {

    private val logger = LoggerFactory.getLogger(TwitterProducer::class.java)

    private val twitterConfig = TwitterConfig

    private val terms = listOf("bitcoin", "use", "politics", "sport", "soccer")
    private val kafkaTopic = "kafka_tweets"

    /**
     * Creates a Twitter client
     *
     * @param msgQueue a queue to store tweets for processing
     * @return a Client instance
     */
    private fun createTwitterClient(msgQueue: BlockingQueue<String>): Client {
        val hosts: Hosts = HttpHosts(Constants.STREAM_HOST)
        val endpoint = StatusesFilterEndpoint()

        endpoint.trackTerms(terms)

        val auth: Authentication =
                OAuth1(twitterConfig.consumerKey,
                        twitterConfig.consumerSecret,
                        twitterConfig.token,
                        twitterConfig.secret)

        val builder: ClientBuilder = ClientBuilder()
                .name("Twitter-Client-01")
                .hosts(hosts)
                .authentication(auth)
                .endpoint(endpoint)
                .processor(StringDelimitedProcessor(msgQueue))

        return builder.build()
    }

    /**
     * Creates a Kafka producer for producing a tweets to Kafka
     *
     * @return a KafkaProducer<String, String> instance
     */
    private fun createKafkaProducer(): KafkaProducer<String, String> {
        val props = mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "127.0.0.1:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,

                // idempotence props
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to "true",
                ProducerConfig.ACKS_CONFIG to "all",
                ProducerConfig.RETRIES_CONFIG to Integer.MAX_VALUE.toString(),
                // if kafka version > 1.1 set to 5, in other case set to 1
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to "5",

                // throughput props
                ProducerConfig.COMPRESSION_TYPE_CONFIG to "snappy",
                ProducerConfig.LINGER_MS_CONFIG to "20",
                ProducerConfig.BATCH_SIZE_CONFIG to (32 * 1024).toString() // 32 KB
        ).toProperties()

        return KafkaProducer(props)
    }

    /**
     * Loads tweets from Twitter and sends them to Kafka topic
     */
    fun run() {
        val msgQueue: BlockingQueue<String> = LinkedBlockingQueue(100000)

        // create a twitter client
        val twitterClient = createTwitterClient(msgQueue)
        twitterClient.connect()

        // create a kafka producer
        val producer = createKafkaProducer()

        Runtime.getRuntime().addShutdownHook(Thread {
            logger.info("Stopping Application...")
            twitterClient.stop()
            producer.close()
        })

        // produce twits into kafka topic
        while (!twitterClient.isDone) {
            var msg: String? = null

            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS)
            } catch (ex: InterruptedException) {
                ex.printStackTrace()
                twitterClient.stop()
            }

            if (msg != null) {
                logger.info("$msg")

                val record = ProducerRecord<String, String>(kafkaTopic, null, msg)

                producer.send(record) { _, error ->
                    if (error != null) {
                        logger.error("Unexpected error", error)
                    }
                }
            }
        }
    }
}

/**
 * Run the producer
 */
fun main() {
    TwitterProducer().run()
}