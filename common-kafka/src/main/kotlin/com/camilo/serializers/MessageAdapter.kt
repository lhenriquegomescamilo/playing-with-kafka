package com.camilo.serializers

import com.camilo.models.Message
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonSerializationContext
import com.google.gson.JsonSerializer
import java.lang.reflect.Type

class MessageAdapter: JsonSerializer<Message<*>> {
    override fun serialize(message: Message<*>, type: Type?, context: JsonSerializationContext?): JsonElement {
        val jsonObject = JsonObject()
        jsonObject.addProperty("type", message.payload?.javaClass?.name)
        jsonObject.add("payload", context?.serialize(message.payload))
        jsonObject.add("correlationId", context?.serialize(message.id))
        return jsonObject
    }

}
