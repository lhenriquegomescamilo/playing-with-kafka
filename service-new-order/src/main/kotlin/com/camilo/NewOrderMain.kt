package com.camilo

import com.camilo.models.CorrelationId
import com.camilo.models.Email
import com.camilo.models.Order
import java.util.*

class NewOrderMain

/**
 * DEPRECREATED
 */
@Deprecated("Use HttpEcommerceService, this is will be discontinued")
fun main() {
    KafkaDispatcher<Order>().use { orderDispatcher ->
        val email = "${randomEmail()}@email.com"
        KafkaDispatcher<Email>().use { emailDisatcher ->
            for (i in 1..10) {
                val orderId = UUID.randomUUID().toString()
                val amount = Math.random() * 5000 + 1

                val order = Order(
                    orderId = orderId,
                    amount = amount.toBigDecimal(),
                    email = email
                )
                val body = "Thank you for order! We are processing your order!!!"

                orderDispatcher.send("ECOMMERCE_NEW_ORDER",
                    email,
                    order,
                    CorrelationId(NewOrderMain::class.java.simpleName))
                emailDisatcher.send("ECOMMERCE_SEND_EMAIL",
                    email,
                    Email(email, body),
                    CorrelationId(NewOrderMain::class.java.simpleName))
            }
        }
    }


}

fun randomEmail(): String = Math.random().toString()
