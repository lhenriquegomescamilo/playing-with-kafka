package com.camilo

import com.camilo.models.Message
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer

interface KafkaBaseService<E, T> {

    fun parser(record: ConsumerRecord<E, Message<T>>)

    fun subscribing(consumer: KafkaConsumer<E, Message<T>>, topic: E)
}