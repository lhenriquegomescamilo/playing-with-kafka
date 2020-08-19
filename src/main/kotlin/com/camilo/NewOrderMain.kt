package com.camilo

import com.camilo.models.Email
import com.camilo.models.Order
import java.util.*

class NewOrderMain

fun main(args: Array<String>) {
    KafkaDispatcher<Order>().use { orderDispatcher ->
        KafkaDispatcher<Email>().use { emailDisatcher ->
            for (i in 1..10) {
                val userId = UUID.randomUUID().toString()
                val orderId = UUID.randomUUID().toString()
                val amount = Math.random() * 5000 + 1
                val order = Order(
                    userId = userId,
                    orderId = orderId,
                    amount = amount.toBigDecimal()
                )
                val value = "$userId,123123,1231"
                val body = "Thank you for order! We are processing your order!!!"
                orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order)
                emailDisatcher.send("ECOMMERCE_SEND_EMAIL", userId, Email("aaaa@aaa.com", body))
            }
        }
    }


}
