package com.camilo

import com.camilo.models.CorrelationId
import com.camilo.models.Email
import com.camilo.models.Message
import com.camilo.models.Order
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.*

class EmailNewOrderService(
    private val emailNewOrderDispatcher: KafkaDispatcher<Email> = KafkaDispatcher(),
) : KafkaBaseService<String, Order> {
    override fun parser(record: ConsumerRecord<String, Message<Order>>) {
        val value = record.value()
        val order = value.payload
        println("-----------------------------------------------")
        println("Processing new order, preparing email")
        println(record.key())
        println(record.topic())
        println(record.partition())
        println(record.offset())
        val email = order.email
        val bodyMessageEmail = "Thank you for order! We are processing your order!!!"
        emailNewOrderDispatcher.sendSync(
            topic = "ECOMMERCE_SEND_EMAIL",
            key = email,
            payload = Email(email, bodyMessageEmail),
            correlationId = CorrelationId(EmailNewOrderService::class.java.simpleName)
        )
        println("Email processed")
        println("-----------------------------------------------")
    }

    override fun subscribing(consumer: KafkaConsumer<String, Message<Order>>, topic: String) {
        consumer.subscribe(Collections.singletonList(topic))
    }
}

fun main() {
    val emailNewOrderService = EmailNewOrderService()
    KafkaService(
        topic = "ECOMMERCE_NEW_ORDER",
        groupId = EmailNewOrderService::class.java.simpleName,
        parser = emailNewOrderService::parser,
        subscribing = emailNewOrderService::subscribing,
        type = Order::class.java
    ).use { it.run() }
}
