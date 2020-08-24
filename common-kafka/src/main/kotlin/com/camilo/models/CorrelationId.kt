package com.camilo.models

import java.util.*

data class CorrelationId(
    private val title: String,
    val id: String = "${title}:(${UUID.randomUUID()})",
)