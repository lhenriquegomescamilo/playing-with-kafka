package com.camilo

import com.camilo.serializers.GsonSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.io.Closeable
import java.util.*
import java.util.Objects.nonNull

class KafkaDispatcher<T> : Closeable {
    private val producer: KafkaProducer<String, T> = KafkaProducer(mainProperties())


    private fun mainProperties(): Properties {
        val properties = Properties()
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer::class.java.name)
        return properties
    }


    private fun handlerMessage(): (RecordMetadata, Exception?) -> Unit {
        return { data, ex ->
            if (nonNull(ex)) {
                ex!!.printStackTrace()
            } else {
                println("Sucesso enviando: ${data.topic()} :: partition ${data.partition()} / offset ${data.offset()} /timestamp ${data.timestamp()}")
            }

        }
    }

    fun send(topic: String, key: String, value: T) {
        val record = ProducerRecord(topic, key, value)
        producer.send(record, handlerMessage()).get()
    }

    override fun close() {
        producer.close()
    }


}