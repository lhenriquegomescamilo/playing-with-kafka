package com.camilo

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*
import java.util.Objects.nonNull

class NewOrderMain

fun main(args: Array<String>) {
    val producer = KafkaProducer<String, String>(mainProperties())
    val value = "12312321,123123,1231"
    val record = ProducerRecord("ECOMMERCE_NEW_ORDER", value, value)
    producer.send(record, handlerMessage()).get()
}

private fun handlerMessage(): (RecordMetadata, Exception?) -> Unit {
    return { data, ex ->
        if (nonNull(ex)) {
            ex?.printStackTrace()
        } else {
            println("Sucesso enviando: ${data.topic()} :: partition ${data.partition()} / offset ${data.offset()} /timestamp ${data.timestamp()}")
        }

    }
}

fun mainProperties(): Properties {
    val properties = Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    return properties
}
