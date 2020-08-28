package com.camilo

import com.camilo.consumer.ConsumerService
import com.camilo.consumer.ServiceRunner
import com.camilo.models.Email
import com.camilo.models.Message
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.*

class EmailService : KafkaBaseService<String, Email>(), ConsumerService<Email> {
    override fun parser(record: ConsumerRecord<String, Message<Email>>) {
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


    override fun subscribing(consumer: KafkaConsumer<String, Message<Email>>, topic: String) {
        consumer.subscribe(Collections.singletonList(topic))
    }

    override fun getTopic(): String {
        return "ECOMMERCE_SEND_EMAIL"
    }

    override fun getConsumerGroup(): String {
        return EmailService::class.java.simpleName
    }

}

fun main() {
    ServiceRunner(::EmailService).start(numberOfThreads = 3)

}
