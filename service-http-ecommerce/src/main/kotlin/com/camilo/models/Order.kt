package com.camilo.models

import java.math.BigDecimal

data class Order(
    var orderId: String = "",
    val amount: BigDecimal,
    val email: String,
)