package com.camilo.serializers

import com.camilo.models.Message
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import org.apache.kafka.common.serialization.Serializer

class GsonSerializer<T>(
    private val gson: Gson = GsonBuilder().registerTypeAdapter(Message::class.java, MessageAdapter()).create()
) : Serializer<T> {
    override fun serialize(topic: String?, data: T) = gson.toJson(data).toByteArray()
}
