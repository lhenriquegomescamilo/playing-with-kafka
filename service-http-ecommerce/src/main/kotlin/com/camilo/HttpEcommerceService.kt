package com.camilo

import com.camilo.models.Email
import com.camilo.models.Order
import com.fasterxml.jackson.databind.SerializationFeature
import io.ktor.application.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.jackson.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.util.*
import java.util.*

val orderDispatcher = KafkaDispatcher<Order>()
val emailDisatcher = KafkaDispatcher<Email>()

@KtorExperimentalAPI
fun main() {
    val server = embeddedServer(Netty, 8081) {
        install(ContentNegotiation) {
            jackson {
                enable(SerializationFeature.INDENT_OUTPUT)
            }
        }

        routing {
            get("/health") { call.respond("I am alive") }
            post("/new") { handleOrderRequest(call) }
        }
    }

    server.start(wait = true)
    server.addShutdownHook {
        arrayOf(orderDispatcher, emailDisatcher).forEach { kafkaDispatcher -> kafkaDispatcher.close() }
    }
}

private suspend fun handleOrderRequest(call: ApplicationCall) {
    val orderRequest = call.receive<Order>()
    sendOrderToKafka(orderRequest)
    call.respond(HttpStatusCode.NoContent)
    println("New order sent successfully")
}

fun sendOrderToKafka(order: Order) {
    val email = order.email
    val orderId = UUID.randomUUID().toString()
    order.orderId = orderId
    orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order)
    emailDisatcher.send("ECOMMERCE_SEND_EMAIL", email, Email(email))

}