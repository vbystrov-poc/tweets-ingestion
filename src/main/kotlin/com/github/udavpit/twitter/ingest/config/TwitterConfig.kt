package com.github.udavpit.twitter.ingest.config

import com.typesafe.config.ConfigFactory

/**
 * Configuration holder for twitter client
 */
object TwitterConfig {

    val consumerKey: String
    val consumerSecret: String
    val token: String
    val secret: String

    init {
        val conf = ConfigFactory.load()
        val twitterClientConfig = conf.getConfig("twitter-client")

        consumerKey = twitterClientConfig.getString("consumer-key")
        consumerSecret = twitterClientConfig.getString("consumer-secret")
        token = twitterClientConfig.getString("token")
        secret = twitterClientConfig.getString("secret")
    }
}