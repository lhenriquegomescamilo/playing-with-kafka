package com.camilo

import com.camilo.models.CorrelationId
import com.camilo.models.Email
import com.camilo.models.Order
import com.camilo.models.User
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
val userDispatcher = KafkaDispatcher<User>()
val batchDispatcher = KafkaDispatcher<String>()

class HttpEcommerceService

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
            get("/reports") { generateAllReports(call) }
        }
    }

    server.start(wait = true)
    server.addShutdownHook {
        arrayOf(orderDispatcher, emailDisatcher, userDispatcher).forEach { kafkaDispatcher -> kafkaDispatcher.close() }
    }
}

private suspend fun generateAllReports(call: ApplicationCall) {
    batchDispatcher.sendSync("ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
        "ECOMMERCE_USER_GENERATE_READING_REPORT",
        "ECOMMERCE_USER_GENERATE_READING_REPORT",
        CorrelationId(HttpEcommerceService::class.java.simpleName))
    println("Sent generate report to all users")
    call.respond(HttpStatusCode.NoContent)

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
    orderDispatcher.sendSync("ECOMMERCE_NEW_ORDER",
        email,
        order,
        CorrelationId(HttpEcommerceService::class.java.simpleName))
    emailDisatcher.sendSync("ECOMMERCE_SEND_EMAIL",
        email,
        Email(email),
        CorrelationId(HttpEcommerceService::class.java.simpleName))

}