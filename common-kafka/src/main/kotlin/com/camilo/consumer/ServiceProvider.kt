package com.camilo.consumer

import com.camilo.KafkaService
import java.util.concurrent.Callable

class ServiceProvider<T>(
    val factory: () -> ConsumerService<T>,
) : Callable<Unit> {

    override fun call() {
        val service = factory();
        KafkaService(
            topic = service.getTopic(),
            groupId = service.getConsumerGroup(),
            parser = service::parser,
            subscribing = service::subscribing,
            propertiesExtras = service.getPropertiesExtras()
        ).use { it.run() }
    }


}
