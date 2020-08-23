package com.camilo.models

import java.util.*

data class CorrelationId(val id: String = UUID.randomUUID().toString())