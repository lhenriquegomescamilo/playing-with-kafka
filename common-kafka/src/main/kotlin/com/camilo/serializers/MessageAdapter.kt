package com.camilo.serializers

import com.camilo.models.CorrelationId
import com.camilo.models.Message
import com.google.gson.*
import java.lang.reflect.Type

class MessageAdapter : JsonSerializer<Message<*>>, JsonDeserializer<Message<*>> {
    override fun serialize(message: Message<*>, type: Type?, context: JsonSerializationContext?): JsonElement {
        val jsonObject = JsonObject()
        jsonObject.addProperty("type", message.payload?.javaClass?.name)
        jsonObject.add("payload", context?.serialize(message.payload))
        jsonObject.add("correlationId", context?.serialize(message.id))
        return jsonObject
    }

    override fun deserialize(jsonElement: JsonElement, type: Type, context: JsonDeserializationContext): Message<*> {
        val obj = jsonElement.asJsonObject
        val payloadType = obj.get("type").asString
        val correlationId = context.deserialize<CorrelationId>(obj.get("correlationId"), CorrelationId::class.java)
        val payload = context.deserialize<Any>(obj.get("payload"), Class.forName(payloadType))
        return Message(id = correlationId, payload = payload)
    }

}
