package com.camilo

import java.util.concurrent.Executors

class ServiceRunner<T>(
    factory: () -> ConsumerService<T>,
    private val serviceProvider: ServiceProvider<T> = ServiceProvider(factory),
) {


    fun start(numberOfThreads: Int) {
        val pool = Executors.newFixedThreadPool(numberOfThreads)
        for (a in 1..numberOfThreads) {
            pool.submit(serviceProvider)
        }
    }

}
