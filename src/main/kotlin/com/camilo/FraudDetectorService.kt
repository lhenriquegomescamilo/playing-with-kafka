package com.camilo

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*

class FraudDetectorService

fun main(vararg: Array<String>) {
    val consumer = KafkaConsumer<String, String>(fraudDetectorProperties())
    consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"))
    while (true) {
        val records = consumer.poll(Duration.ofMillis(100))
        if (!records.isEmpty) {
            println("I found ${records.count()} records")
            for (record in records) {
                println("-----------------------------------------------")
                println("Processing new order, checking for fraud")
                println(record.key())
                println(record.value())
                println(record.topic())
                println(record.partition())
                println(record.offset())
                println("-----------------------------------------------")
                Thread.sleep(Duration.ofSeconds(5).toMillis())
                println("Order processed")
            }
        }
    }
}

private fun fraudDetectorProperties(): Properties {
    val properties = Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService::class.java.simpleName)
    properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, FraudDetectorService::class.java.simpleName)
    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")

    return properties
}