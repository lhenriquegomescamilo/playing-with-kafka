package com.camilo

import com.camilo.batch.IO
import com.camilo.consumer.ConsumerService
import com.camilo.consumer.ServiceRunner
import com.camilo.models.Message
import com.camilo.models.User
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.io.File
import java.nio.file.Path
import java.util.*

class ReadingReportService : KafkaBaseService<String, User>(), ConsumerService<User> {
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

    override fun getTopic() = "ECOMMERCE_USER_GENERATE_READING_REPORT"
    override fun getConsumerGroup(): String = ReadingReportService::class.java.simpleName
}

fun main() {
    ServiceRunner(::ReadingReportService).start(numberOfThreads = 3)
}
