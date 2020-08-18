package com.camilo

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.*

class FraudDetectorService {
    fun parser(record: ConsumerRecord<String, String>) {
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


    fun subscribing(consumer: KafkaConsumer<String, String>, topic: String) {
        consumer.subscribe(Collections.singletonList(topic))
    }
}

fun main(vararg: Array<String>) {
    val fraudDetectorService = FraudDetectorService()
    KafkaService(
        groupId = FraudDetectorService::class.java.simpleName,
        topic = "ECOMMERCE_NEW_ORDER",
        parser = fraudDetectorService::parser,
        subscribing = fraudDetectorService::subscribing
    ).use { it.run() }
}
