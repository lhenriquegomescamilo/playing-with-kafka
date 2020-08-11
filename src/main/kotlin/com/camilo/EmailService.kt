package com.camilo

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*

class EmailService

fun main(vararg: Array<String>) {
    val consumer = KafkaConsumer<String, String>(fraudDetectorProperties())
    consumer.subscribe(Collections.singletonList("ECOMMERCE_SEND_EMAIL"))
    while (true) {
        val records = consumer.poll(Duration.ofMillis(100))
        if (!records.isEmpty) {
            println("I found ${records.count()} records")
            for (record in records) {
                println("-----------------------------------------------")
                println("Send email")
                println(record.key())
                println(record.value())
                println(record.topic())
                println(record.partition())
                println(record.offset())
                println("-----------------------------------------------")
                Thread.sleep(Duration.ofSeconds(1).toMillis())
                println("Email sent")
            }
        }
    }
}

private fun fraudDetectorProperties(): Properties {
    val properties = Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailService::class.java.simpleName)
    return properties
}