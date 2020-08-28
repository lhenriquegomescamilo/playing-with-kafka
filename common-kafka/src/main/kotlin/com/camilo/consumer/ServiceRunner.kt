package com.camilo.consumer

import java.util.concurrent.Executors

class ServiceRunner<T, E>(
    factory: () -> ConsumerService<T, E>,
    private val serviceProvider: ServiceProvider<T, E> = ServiceProvider(factory),
) {


    fun start(numberOfThreads: Int) {
        val pool = Executors.newFixedThreadPool(numberOfThreads)
        for (a in 1..numberOfThreads) {
            pool.submit(serviceProvider)
        }
    }

}
