package com.camilo

import com.camilo.models.Email
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*

class EmailService {
    fun parser(record: ConsumerRecord<String, Email>) {
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

    fun subscribing(consumer: KafkaConsumer<String, Email>, topic: String) {
        consumer.subscribe(Collections.singletonList(topic))
    }

}

fun main() {
    val emailService = EmailService()
    KafkaService(
        topic = "ECOMMERCE_SEND_EMAIL",
        groupId = FraudDetectorService::class.java.simpleName,
        parser = emailService::parser,
        subscribing = emailService::subscribing,
        type = Email::class.java,
        propertiesExtras = mapOf(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name)
    ).use { it.run() }


}
