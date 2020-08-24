package com.camilo

import com.camilo.models.Message
import com.camilo.models.Order
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.sql.Connection
import java.sql.DriverManager
import java.util.*

class CreateUserService(
    private val connection: Connection = DriverManager.getConnection("jdbc:sqlite:target/users_database.db")
) : KafkaBaseService<String, Order> {
    init {
        try {
            connection.createStatement()
                .execute("CREATE TABLE Users( uuid varchar(200) primary key, email varchar(200))")
        } catch (e: Exception) {
            // be careful, the sql could be wrong
            e.printStackTrace()
        }
    }

    override fun parser(record: ConsumerRecord<String, Message<Order>>) {

        println("-----------------------------------------------")
        println("Processing new order, checking for new user")
        val value = record.value()
        val order = value.payload
        if (isNewUser(order.email)) {
            insertNewUser(order.email)
            println("User ${order.email} was created")
        } else {
            println("User with email ${order.email} already exists")
        }
        println("-----------------------------------------------")
    }

    private fun isNewUser(email: String): Boolean {
        val exists = connection.prepareStatement("select uuid from Users where email = ? limit 1 ")
        exists.setString(1, email)
        val result = exists.executeQuery()
        return !result.next()
    }

    private fun insertNewUser(email: String) {
        val insert = connection.prepareStatement("insert into Users (uuid, email) values (?,?)")
        insert.setString(1, UUID.randomUUID().toString())
        insert.setString(2, email)
        insert.execute()

    }

    override fun subscribing(consumer: KafkaConsumer<String, Message<Order>>, topic: String) {
        consumer.subscribe(Collections.singletonList(topic))
    }
}

fun main() {
    val createUserService = CreateUserService()
    KafkaService(
        topic = "ECOMMERCE_NEW_ORDER",
        groupId = CreateUserService::class.java.simpleName,
        parser = createUserService::parser,
        subscribing = createUserService::subscribing,
        type = Order::class.java
    ).use { it.run() }
}
