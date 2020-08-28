package com.camilo

import com.camilo.models.Message
import com.camilo.models.User
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.sql.Connection
import java.sql.DriverManager
import java.util.*

class BatchSendMessageService(
    private val connection: Connection = DriverManager.getConnection("jdbc:sqlite:target/users_database.db"),
    private val userDispatcher: KafkaDispatcher<User> = KafkaDispatcher(),
) : KafkaBaseService<String, String>() {


    init {
        try {
            connection.createStatement()
                .execute("CREATE TABLE Users( uuid varchar(200) primary key, email varchar(200))")
        } catch (e: Exception) {
            // be careful, the sql could be wrong
            e.printStackTrace()
        }
    }

    override fun parser(record: ConsumerRecord<String, Message<String>>) {
        val message = record.value()
        println("-----------------------------------------------")
        println("Processing new batch")
        println("Topic : $message")
        for (user in findAllUsers()) {
            userDispatcher.sendAsync(
                record.value().payload,
                user.uuid,
                user,
                message.id.continueWith(BatchSendMessageService::class.java.simpleName)
            )
            println("Sending to user ${user.uuid}")
        }
        println("Users processed")
    }

    private fun findAllUsers(): List<User> {
        val results = connection.prepareStatement("select uuid from Users").executeQuery()
        val users = LinkedList<User>();
        while (results.next()) users.add(User(results.getString(1)))
        return users
    }

    override fun subscribing(consumer: KafkaConsumer<String, Message<String>>, topic: String) {
        consumer.subscribe(Collections.singletonList(topic))
    }
}


fun main() {
    val batchSendMessageService = BatchSendMessageService()
    KafkaService(
        topic = "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
        groupId = CreateUserService::class.java.simpleName,
        parser = batchSendMessageService::parser,
        subscribing = batchSendMessageService::subscribing,
        type = String::class.java
    ).use { it.run() }
}