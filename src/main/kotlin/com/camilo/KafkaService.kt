package com.camilo

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*

class KafkaService(
    topic: String,
    private val parser: (ConsumerRecord<String, String>) -> Unit
) {
    private val consumer: KafkaConsumer<String, String>

    init {
        consumer = KafkaConsumer(properties())
        consumer.subscribe(Collections.singletonList(topic))
    }

    fun run() {
        while (true) {
            val records = consumer.poll(Duration.ofMillis(100))
            if (!records.isEmpty) {
                println("I found ${records.count()} records")
                for (record in records) {
                    parser(record)
                }
            }
        }

    }

    private fun properties(): Properties {
        val properties = Properties()
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailService::class.java.simpleName)
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
        return properties
    }
}