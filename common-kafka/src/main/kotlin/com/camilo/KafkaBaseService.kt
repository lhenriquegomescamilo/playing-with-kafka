package com.camilo

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer

interface KafkaBaseService<E, T> {
    fun parser(record: ConsumerRecord<E, T>)

    fun subscribing(consumer: KafkaConsumer<E, T>, topic: E)
}