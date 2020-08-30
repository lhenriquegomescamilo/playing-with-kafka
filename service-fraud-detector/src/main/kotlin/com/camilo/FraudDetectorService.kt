package com.camilo

import com.camilo.consumer.ConsumerService
import com.camilo.consumer.ServiceRunner
import com.camilo.models.Message
import com.camilo.models.Order
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.math.BigDecimal
import java.time.Duration
import java.util.*

class FraudDetectorService(
    private val orderDispatcher: KafkaDispatcher<Order> = KafkaDispatcher(),
    private val localDatabase: LocalDatabase = LocalDatabase(databaseName = "frauds_database"),
) : KafkaBaseService<String, Order>(), ConsumerService<Order> {

    init {
        localDatabase.createIfNotExists(sql = "create table Orders(uuid varchar(200) primary key, is_fraud boolean)")
    }

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
        if (wasNotProcessed(order)) {
            detectorFraud(order, value)
            println("Order processed")
        } else {
            println("Order ${order.orderId} was already processed")
        }
        println("-----------------------------------------------")
    }

    private fun wasNotProcessed(order: Order): Boolean {
        return localDatabase.query("select uuid from Orders where uuid = ? limit 1", order.orderId).next().not()
    }

    private fun detectorFraud(order: Order, message: Message<Order>) {
        if (isFraud(order)) {
            localDatabase.updateStateDatabase("insert into Orders(uuid,is_fraud) values(?, true)", order.orderId)

            println("Order $order is a fraud!!!")
            orderDispatcher.sendSync(
                topic = "ECOMMERCE_ORDER_REJECTED",
                key = order.email,
                payload = order,
                correlationId = message.id.continueWith(FraudDetectorService::class.java.simpleName))
        } else {
            localDatabase.updateStateDatabase("insert into Orders(uuid,is_fraud) values(?, false)", order.orderId)
            orderDispatcher.sendSync(
                topic = "ECOMMERCE_ORDER_APPROVED",
                key = order.email,
                payload = order,
                correlationId = message.id.continueWith(FraudDetectorService::class.java.simpleName)
            )
            println("Approved order $order")
        }
    }

    private fun isFraud(value: Order) = value.amount > BigDecimal("4500")


    override fun subscribing(consumer: KafkaConsumer<String, Message<Order>>, topic: String) {
        consumer.subscribe(Collections.singletonList(topic))
    }

    override fun getTopic() = "ECOMMERCE_NEW_ORDER"

    override fun getConsumerGroup() = FraudDetectorService::class.java.simpleName
}

fun main() {
    ServiceRunner(::FraudDetectorService).start(numberOfThreads = 1)
}
