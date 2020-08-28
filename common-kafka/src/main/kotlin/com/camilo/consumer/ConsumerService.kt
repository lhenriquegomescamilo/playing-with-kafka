package com.camilo.consumer

import com.camilo.models.Message
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer

interface ConsumerService<T> {
    fun parser(record: ConsumerRecord<String, Message<T>>)

    fun getTopic(): String

    fun getConsumerGroup(): String

    fun subscribing(consumer: KafkaConsumer<String, Message<T>>, topic: String)
}