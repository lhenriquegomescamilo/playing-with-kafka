package com.camilo.models

data class Message<T>(val id: CorrelationId, val payload: T)