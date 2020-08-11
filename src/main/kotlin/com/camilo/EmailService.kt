package com.camilo

import org.apache.kafka.clients.consumer.ConsumerRecord
import java.time.Duration

class EmailService {
    fun parser(record: ConsumerRecord<String, String>){
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

fun main(vararg: Array<String>) {
    val emailService = EmailService()
    val kafkaService = KafkaService("ECOMMERCE_NEW_ORDER", emailService::parser)
    kafkaService.run()

}
