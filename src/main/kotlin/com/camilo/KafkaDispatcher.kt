package com.camilo

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.io.Closeable
import java.util.*

class KafkaDispatcher : Closeable {
    private val producer: KafkaProducer<String, String> = KafkaProducer(mainProperties())


    private fun mainProperties(): Properties {
        val properties = Properties()
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        return properties
    }


    private fun handlerMessage(): (RecordMetadata, Exception?) -> Unit {
        return { data, ex ->
            if (Objects.nonNull(ex)) {
                ex?.printStackTrace()
            } else {
                println("Sucesso enviando: ${data.topic()} :: partition ${data.partition()} / offset ${data.offset()} /timestamp ${data.timestamp()}")
            }

        }
    }

    fun send(topic: String, key: String, value: String) {
        val record = ProducerRecord(topic, key, value)
        producer.send(record, handlerMessage()).get()
    }

    override fun close() {
        producer.close()
    }


}