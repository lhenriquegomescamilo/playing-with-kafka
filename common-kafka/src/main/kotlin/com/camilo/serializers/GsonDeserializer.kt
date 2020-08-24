package com.camilo.serializers

import com.camilo.models.Message
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import org.apache.kafka.common.serialization.Deserializer

class GsonDeserializer(
    private val gson: Gson = GsonBuilder().registerTypeAdapter(Message::class.java, MessageAdapter()).create()
) : Deserializer<Message<*>> {

    override fun deserialize(topic: String, data: ByteArray): Message<*> {
        return gson.fromJson(String(data), Message::class.java)
    }


}