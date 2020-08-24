package com.camilo

import com.camilo.batch.IO
import com.camilo.models.Message
import com.camilo.models.User
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.io.File
import java.nio.file.Path
import java.util.*

class ReadingReportService : KafkaBaseService<String, User> {
    companion object {
        val source: Path = File("src/main/resources/report.txt").toPath()
    }

    override fun parser(record: ConsumerRecord<String, Message<User>>) {
        val value = record.value()
        val user = value.payload
        println("-----------------------------------------------")
        println("Processing report for user $user")

        val target = File(user.reportPath())
        IO.copyTo(source, target)
        IO.append(target, "\nCreated for ${user.uuid}")
        println("File created: ${target.absolutePath}")
        println("-----------------------------------------------")
    }


    override fun subscribing(consumer: KafkaConsumer<String, Message<User>>, topic: String) {
        consumer.subscribe(Collections.singletonList(topic))
    }
}

fun main() {
    val readingReportService = ReadingReportService()
    KafkaService(
        topic = "ECOMMERCE_USER_GENERATE_READING_REPORT",
        groupId = ReadingReportService::class.java.simpleName,
        parser = readingReportService::parser,
        subscribing = readingReportService::subscribing,
        type = User::class.java
    ).use { it.run() }
}
