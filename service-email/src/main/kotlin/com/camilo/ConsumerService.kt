package com.camilo

import com.camilo.models.Email
import com.camilo.models.Message
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer

interface ConsumerService<T> {
    fun parser(record: ConsumerRecord<String, Message<Email>>)

    fun getTopic(): String

    fun getConsumerGroup(): String

    fun subscribing(consumer: KafkaConsumer<String, Message<Email>>, topic: String)
}