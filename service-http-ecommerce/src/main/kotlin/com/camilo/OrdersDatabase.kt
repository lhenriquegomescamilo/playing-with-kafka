package com.camilo

import com.camilo.models.Order
import java.io.Closeable

class OrdersDatabase(
    private val localDatabase: LocalDatabase = LocalDatabase(databaseName = "http_ecommerce_database"),
) : Closeable {

    init {
        localDatabase.createIfNotExists(sql = "create table Orders(uuid varchar(200) primary key)")
    }

    fun saveNewOrder(order: Order): Boolean = if (wasProcessed(order)) {
        false
    } else {
        localDatabase.updateStateDatabase("insert into Orders(uuid) values(?)", order.orderId)
        true
    }

    private fun wasProcessed(order: Order): Boolean {
        //language=SQLite
        return localDatabase.query(
            query = "select uuid from Orders where uuid = ? limit 1",
            order.orderId).next()
    }

    override fun close() {
        localDatabase.close();
    }
}
