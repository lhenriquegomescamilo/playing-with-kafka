package com.camilo

import com.camilo.consumer.ConsumerService
import com.camilo.consumer.ServiceRunner
import com.camilo.models.Message
import com.camilo.models.Order
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.sql.Connection
import java.sql.DriverManager
import java.util.*

class CreateUserService(
    private val connection: Connection = DriverManager.getConnection("jdbc:sqlite:target/users_database.db"),
    val localDatabase: LocalDatabase = LocalDatabase(databaseName = "users_database"),
) : KafkaBaseService<String, Order>(), ConsumerService<Order> {

    init {
        localDatabase.createIfNotExists("CREATE TABLE Users( uuid varchar(200) primary key, email varchar(200))")
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

    private fun insertNewUser(email: String) {
        val statement = "insert into Users (uuid, email) values (?,?)"
        val uuid = UUID.randomUUID().toString()
        localDatabase.updateStateDatabase(statement, uuid, email)
        println("Created user $uuid  with e-mail $email")

    }


    private fun isNewUser(email: String): Boolean {
        val query = "select uuid from Users where email = ? limit 1 "
        val result = localDatabase.query(query, email)
        return !result.next()
    }


    override fun subscribing(consumer: KafkaConsumer<String, Message<Order>>, topic: String) {
        consumer.subscribe(Collections.singletonList(topic))
    }

    override fun getTopic() = "ECOMMERCE_NEW_ORDER"

    override fun getConsumerGroup() = CreateUserService::class.java.simpleName
}

fun main() {
    ServiceRunner(::CreateUserService).start(numberOfThreads = 1)
}
