package com.camilo

import com.camilo.models.Message
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.regex.Pattern

class LogService : KafkaBaseService<String, String> {
    override fun parser(record: ConsumerRecord<String, Message<String>>) {
        println("-----------------------------------------------")
        println("LOG")
        println(record.key())
        println(record.value())
        println(record.topic())
        println(record.partition())
        println(record.offset())
        println("-----------------------------------------------")
    }

    override fun subscribing(consumer: KafkaConsumer<String, Message<String>>, topic: String) {
        consumer.subscribe(Pattern.compile(topic))
    }
}

fun main() {
    val logService = LogService()
    val kafkaService = KafkaService(
        topic = "ECOMMERCE.*",
        groupId = LogService::class.java.simpleName,
        parser = logService::parser,
        subscribing = logService::subscribing,
        type = String::class.java,
        propertiesExtras = mapOf(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name)
    )
    kafkaService.run()
}
