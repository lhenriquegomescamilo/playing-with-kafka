package com.camilo

import com.camilo.models.Message
import com.camilo.serializers.GsonDeserializer
import com.camilo.serializers.GsonSerializer
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
    val type: Class<T>? = null,
    val propertiesExtras: Map<String, String>? = emptyMap(),
) : Closeable {
    private val consumer: KafkaConsumer<String, Message<T>>

    init {
        consumer = KafkaConsumer(properties(groupId))
        subscribing(consumer, topic)
    }

    fun run() {
        KafkaDispatcher<ByteArray>().use { deadLetterDispatcher ->
            while (true) {
                val records = consumer.poll(Duration.ofMillis(100))
                if (!records.isEmpty) {
                    println("I found ${records.count()} records")
                    for (record in records) {
                        parseRecord(record, deadLetterDispatcher)
                    }
                }
            }
        }

    }

    private fun parseRecord(
        record: ConsumerRecord<String, Message<T>>,
        deadLetterDispatcher: KafkaDispatcher<ByteArray>,
    ) {
        try {
            parser(record)
        } catch (e: Exception) {
            println("Something happened")
            e.printStackTrace()
            val message = record.value()
            deadLetterDispatcher.sendSync(
                topic = "ECOMMERCE_DEAD_LETTER",
                key = message.id.toString(),
                payload = GsonSerializer<String>().serialize("", message.id.toString()),
                correlationId = message.id.continueWith("DeadLetter"),
            )
        }
    }

    private fun properties(groupId: String): Properties {
        val properties = Properties()
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9090")
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString())
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

        propertiesExtras?.let { properties.putAll(it.toMap()) }
        return properties
    }

    override fun close() {
        this.consumer.close()
    }
}