package com.camilo

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet

class LocalDatabase(
    private val databaseName: String,
    private val connection: Connection = DriverManager.getConnection("jdbc:sqlite:target/${databaseName}.db"),
) {
    // yes, this is way too generic
    // this methodo possible sql inejction
    fun createIfNotExists(sql: String) {
        try {
            connection.createStatement().execute(sql);
        } catch (ignored: Exception) {
        }
    }

    fun updateStateDatabase(statement: String, vararg params: String): Boolean {
        return preparedStatement(statement, params).execute()
    }

    private fun preparedStatement(
        statement: String,
        params: Array<out String>,
    ): PreparedStatement {
        val preparedStatement = connection.prepareStatement(statement);
        applyParameters(params, preparedStatement)
        return preparedStatement
    }

    private fun applyParameters(params: Array<out String>, preparedStatement: PreparedStatement) {
        for ((index, value) in params.withIndex()) {
            preparedStatement.setString(index + 1, value)
        }
    }

    fun query(query: String, vararg params: String): ResultSet {
        return preparedStatement(query, params).executeQuery()
    }

    fun close() = connection.close()

}