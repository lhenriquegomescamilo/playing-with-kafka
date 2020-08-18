package com.camilo

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.*

class EmailService {
    fun parser(record: ConsumerRecord<String, String>) {
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

    fun subscribing(consumer: KafkaConsumer<String, String>, topic: String) {
        consumer.subscribe(Collections.singletonList(topic))
    }

}

fun main(vararg: Array<String>) {
    val emailService = EmailService()
    KafkaService(
        groupId = FraudDetectorService::class.java.simpleName,
        topic = "ECOMMERCE_SEND_EMAIL",
        parser = emailService::parser,
        subscribing = emailService::subscribing
    ).use { it.run() }


}
