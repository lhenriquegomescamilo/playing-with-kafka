package com.camilo

import com.camilo.models.Message
import com.camilo.serializers.GsonDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.io.Closeable
import java.time.Duration
import java.util.*

class KafkaService<T>(
    val topic: String,
    val groupId: String,
    private val parser: (ConsumerRecord<String, Message<T>>) -> Unit,
    val subscribing: (KafkaConsumer<String, Message<T>>, String) -> Unit,
    val type: Class<T>,
    val propertiesExtras: Map<String, String>? = emptyMap()
) : Closeable {
    private val consumer: KafkaConsumer<String, Message<T>>

    init {
        consumer = KafkaConsumer(properties(groupId))
        subscribing(consumer, topic)
    }

    fun run() {
        while (true) {
            val records = consumer.poll(Duration.ofMillis(100))
            if (!records.isEmpty) {
                println("I found ${records.count()} records")
                for (record in records) {
                    parser(record)
                }
            }
        }

    }

    private fun properties(groupId: String): Properties {
        val properties = Properties()
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9090")
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString())
        propertiesExtras?.let { properties.putAll(it.toMap()) }
        return properties
    }

    override fun close() {
        this.consumer.close()
    }
}