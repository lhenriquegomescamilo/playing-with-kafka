package com.camilo.models

data class User(
    val uuid: String
) {
    fun reportPath(): String {
        return "target/$uuid-report.txt"
    }
}