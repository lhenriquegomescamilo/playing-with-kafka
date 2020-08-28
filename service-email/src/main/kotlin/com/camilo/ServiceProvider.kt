package com.camilo

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.concurrent.Callable

class ServiceProvider<T>(
    val factory: () -> ConsumerService<T>
) : Callable<Unit> {

    override fun call() {
        val service = factory();
        KafkaService(
            topic = service.getTopic(),
            groupId = service.getConsumerGroup(),
            parser = service::parser,
            subscribing = service::subscribing,
            propertiesExtras = mapOf(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name)
        ).use { it.run() }
    }


}
