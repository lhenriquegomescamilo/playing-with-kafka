package com.camilo

import com.camilo.models.Email
import com.camilo.models.Order
import io.ktor.application.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import java.util.*

fun main() {
    val server = embeddedServer(Netty, 8080) {
        routing {
            get("/new") {
                val body = "Thank you for order! We are processing your order!!!"
                generateOrder(body)
                call.respond(body)

            }
        }
    }

    server.start(wait = true)
}

fun generateOrder(messageBody: String) {
    KafkaDispatcher<Order>().use { orderDispatcher ->
        val email = "${randomEmail()}@email.com"
        KafkaDispatcher<Email>().use { emailDisatcher ->
            val orderId = UUID.randomUUID().toString()
            val amount = Math.random() * 5000 + 1
            val order = Order(
                orderId = orderId,
                amount = amount.toBigDecimal(),
                email = email
            )
            orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order)
            emailDisatcher.send("ECOMMERCE_SEND_EMAIL", email, Email(email, messageBody))
        }
    }
}


fun randomEmail(): String = Math.random().toString()
