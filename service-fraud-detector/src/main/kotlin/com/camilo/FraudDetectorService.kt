package com.camilo

import com.camilo.models.Order
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.*

class FraudDetectorService {
    fun parser(record: ConsumerRecord<String, Order>) {
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


    fun subscribing(consumer: KafkaConsumer<String, Order>, topic: String) {
        consumer.subscribe(Collections.singletonList(topic))
    }
}

fun main() {
    val fraudDetectorService = FraudDetectorService()
    KafkaService(
        topic = "ECOMMERCE_NEW_ORDER",
        groupId = FraudDetectorService::class.java.simpleName,
        parser = fraudDetectorService::parser,
        subscribing = fraudDetectorService::subscribing,
        type = Order::class.java
    ).use { it.run() }
}
