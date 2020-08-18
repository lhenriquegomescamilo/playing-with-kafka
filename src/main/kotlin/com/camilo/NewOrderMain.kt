package com.camilo

import java.util.*

class NewOrderMain

fun main(args: Array<String>) {
    KafkaDispatcher().use {
        for (i in 1..100) {
            val key = UUID.randomUUID().toString()
            val value = "$key,123123,1231"
            val email = "Thank you for order! We are processing your order!!!"
            it.send("ECOMMERCE_NEW_ORDER", key, value)
            it.send("ECOMMERCE_SEND_EMAIL", key, email)
        }
    }


}
