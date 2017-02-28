package com.simplydistributed.wordpress.kafkastreams.producer;

import java.util.logging.Logger;

/**
 *
 * Entry point for the application. Kicks off Kafka Producer thread
 */
public class ProducerBootstrap {

    private static final Logger LOGGER = Logger.getLogger(ProducerBootstrap.class.getName());

    public static void main(String[] args) throws Exception {

        new Thread(new Producer()).start();
        LOGGER.info("Kafka producer triggered");

    }
}
