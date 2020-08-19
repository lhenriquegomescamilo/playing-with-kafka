package com.camilo.serializers

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import org.apache.kafka.common.serialization.Deserializer
import java.nio.charset.Charset

class GsonDeserializer<T>(private val gson: Gson = GsonBuilder().create()) : Deserializer<T> {
    private lateinit var type: Class<T>

    companion object {
        val TYPE_CONFIG = "com.camilo.type_config"
    }

    override fun deserialize(topic: String?, data: ByteArray?): T {
        return gson.fromJson(data?.toString(Charset.defaultCharset()), type)
    }

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
        try {
            val typeName = configs?.get(TYPE_CONFIG) as String
            this.type = Class.forName(typeName) as Class<T>
        } catch (e: ClassNotFoundException) {
            throw RuntimeException("Type for deserialization not exist in the classpath $e")
        }

    }
}
