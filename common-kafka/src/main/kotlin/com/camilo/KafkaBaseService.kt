package com.camilo

import com.camilo.models.Message
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer

abstract class KafkaBaseService<E, T> {

    abstract fun parser(record: ConsumerRecord<E, Message<T>>)

    abstract fun subscribing(consumer: KafkaConsumer<E, Message<T>>, topic: E)

    fun getPropertiesExtras(): Map<String, String> = emptyMap();


}