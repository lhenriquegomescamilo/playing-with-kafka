package com.camilo

import com.camilo.models.Message
import com.camilo.models.Order
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.math.BigDecimal
import java.time.Duration
import java.util.*

class FraudDetectorService(
    private val orderDispatcher: KafkaDispatcher<Order> = KafkaDispatcher(FraudDetectorService::class.java.simpleName)
) : KafkaBaseService<String, Order> {
    override fun parser(record: ConsumerRecord<String, Message<Order>>) {
        val value = record.value()
        val order = value.payload
        println("-----------------------------------------------")
        println("Processing new order, checking for fraud")
        println(record.key())
        println(record.topic())
        println(record.partition())
        println(record.offset())
        Thread.sleep(Duration.ofSeconds(5).toMillis())
        detectorFraud(order)
        println("Order processed")
        println("-----------------------------------------------")
    }

    private fun detectorFraud(order: Order) {
        if (isFraud(order)) {
            // pretting that the process fraud happens when the amaount is great than 4500
            println("Order $order is a fraud!!!")
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.email, order)
        } else {
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.email, order)
            println("Approved order $order")
        }
    }

    private fun isFraud(value: Order) = value.amount > BigDecimal("4500")


    override fun subscribing(consumer: KafkaConsumer<String, Message<Order>>, topic: String) {
        consumer.subscribe(Collections.singletonList(topic))
    }
}

fun main() {
    val fraudDetectorService = FraudDetectorService()
    KafkaService(
        topic = "ECOMMERCE_NEW_ORDER",
        groupId = FraudDetectorService::class.java.simpleName,
        parser = fraudDetectorService::parser,
        subscribing = fraudDetectorService::subscribing,
        type = Order::class.java
    ).use { it.run() }
}
