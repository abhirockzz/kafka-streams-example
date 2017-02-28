package com.simplydistributed.wordpress.kafkastreams;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

/**
 * Defines K-Streams configuration, builds processing topology and encapsulates
 * the processor logic
 */
public final class CPUMetricStreamHandler {

    private static final Logger LOGGER = Logger.getLogger(CPUMetricStreamHandler.class.getSimpleName());

    private static final String SOURCE_NAME = "cpu-metrics-topic-source";
    private static final String TOPIC_NAME = "cpu-metrics-topic";
    public static final String AVG_STORE_NAME = "in-memory-avg-store";
    private static final String NUM_RECORDS_STORE_NAME = "num-recorsd-store";
    private static final String PROCESSOR_NAME = "avg-processor";

    public KafkaStreams startPipeline() {

        Map<String, Object> configurations = new HashMap<>();

        String streamsAppServerConfig = GlobalAppState.getInstance().getHostPortInfo().host() + ":"
                + GlobalAppState.getInstance().getHostPortInfo().port();

        configurations.put(StreamsConfig.APPLICATION_SERVER_CONFIG, streamsAppServerConfig);

        configurations.put(StreamsConfig.APPLICATION_ID_CONFIG, Optional.ofNullable(System.getenv("STREAMS_APP_ID")).orElse("cpu-streamz"));

        String kafkaBroker = Optional.ofNullable(System.getenv("KAFKA_BROKER")).orElse("localhost:9092");
        LOGGER.log(Level.INFO, "Kafa broker {0}", kafkaBroker);
        configurations.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);

        String zk = Optional.ofNullable(System.getenv("ZOOKEEPER")).orElse("localhost:2181");
        LOGGER.log(Level.INFO, "Zookeeper node {0}", zk);
        configurations.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zk);

        configurations.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        configurations.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        configurations.put(StreamsConfig.STATE_DIR_CONFIG, AVG_STORE_NAME);
        configurations.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500");

        StreamsConfig config = new StreamsConfig(configurations);

        TopologyBuilder builder = processingTopologyBuilder();
        KafkaStreams streams = null;
        boolean connected = false;
        int retries = 0;

        do {
            LOGGER.info("Initiating Kafka Streams");
            try {
                streams = new KafkaStreams(builder, config);
                connected = true;
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error during Kafka Stream initialization {0} .. retrying", e.getMessage());
                retries++;
            }

        } while (!connected && retries <= 1); //retry

        if (!connected) {
            LOGGER.warning("Unable to initialize Kafka Streams.. exiting");
            System.exit(0);
        }

        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOGGER.log(Level.SEVERE, "Uncaught exception in Thread {0} - {1}", new Object[]{t, e.getMessage()});
            }
        });

        streams.start();

        return streams;

    }

    private TopologyBuilder processingTopologyBuilder() {

        StateStoreSupplier machineToAvgCPUUsageStore
                = Stores.create(AVG_STORE_NAME)
                        .withStringKeys()
                        .withDoubleValues()
                        .inMemory()
                        .build();

        StateStoreSupplier machineToNumOfRecordsReadStore
                = Stores.create(NUM_RECORDS_STORE_NAME)
                        .withStringKeys()
                        .withIntegerValues()
                        .inMemory()
                        .build();

        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource(SOURCE_NAME, TOPIC_NAME)
                .addProcessor(PROCESSOR_NAME, new ProcessorSupplier() {
                    @Override
                    public Processor get() {
                        return new CPUCumulativeAverageProcessor();
                    }
                }, SOURCE_NAME)
                .addStateStore(machineToAvgCPUUsageStore, PROCESSOR_NAME)
                .addStateStore(machineToNumOfRecordsReadStore, PROCESSOR_NAME);

        LOGGER.info("Kafka streams processing topology ready");

        return builder;
    }

    public static class CPUCumulativeAverageProcessor implements Processor<String, String> {

        private static final Logger PROC_LOGGER = Logger.getLogger(CPUCumulativeAverageProcessor.class.getSimpleName());

        private ProcessorContext pc;
        private KeyValueStore<String, Double> machineToAvgCPUUsageStore;
        private KeyValueStore<String, Integer> machineToNumOfRecordsReadStore;

        public CPUCumulativeAverageProcessor() {
        }

        @Override
        public void init(ProcessorContext pc) {

            this.pc = pc;
            this.pc.schedule(12000); //invoke punctuate every 12 seconds
            this.machineToAvgCPUUsageStore = (KeyValueStore<String, Double>) pc.getStateStore(AVG_STORE_NAME);
            this.machineToNumOfRecordsReadStore = (KeyValueStore<String, Integer>) pc.getStateStore(NUM_RECORDS_STORE_NAME);

            PROC_LOGGER.info("Processor initialized");
        }

        @Override
        public void process(String machineID, String currentCPUUsage) {
            PROC_LOGGER.log(Level.INFO, "Calculating CMA for machine {0}", machineID);

            //turn each String value (cpu usage) to Double
            Double currentCPUUsageD = Double.parseDouble(currentCPUUsage);
            Integer recordsReadSoFar = machineToNumOfRecordsReadStore.get(machineID);
            Double latestCumulativeAvg = null;

            if (recordsReadSoFar == null) {
                PROC_LOGGER.log(Level.INFO, "First record for machine {0}", machineID);
                machineToNumOfRecordsReadStore.put(machineID, 1);
                latestCumulativeAvg = currentCPUUsageD;
            } else {
                Double cumulativeAvgSoFar = machineToAvgCPUUsageStore.get(machineID);
                PROC_LOGGER.log(Level.INFO, "CMA so far {0}", cumulativeAvgSoFar);

                //refer https://en.wikipedia.org/wiki/Moving_average#Cumulative_moving_average for details
                latestCumulativeAvg = (currentCPUUsageD + (recordsReadSoFar * cumulativeAvgSoFar)) / (recordsReadSoFar + 1);

                recordsReadSoFar = recordsReadSoFar + 1;
                machineToNumOfRecordsReadStore.put(machineID, recordsReadSoFar);
                PROC_LOGGER.log(Level.INFO, "Total records for machine {0} so far {1}", new Object[]{machineID, recordsReadSoFar});

            }

            machineToAvgCPUUsageStore.put(machineID, latestCumulativeAvg); //store latest CMA in local state store

            PROC_LOGGER.log(Level.INFO, "Latest CMA for machine {0} = {1}", new Object[]{machineID, latestCumulativeAvg});

        }

        @Override
        public void punctuate(long l) {

            pc.commit();

        }

        @Override
        public void close() {
            PROC_LOGGER.warning("Closing processor...");
            machineToAvgCPUUsageStore.close();
            machineToNumOfRecordsReadStore.close();
        }

    }
}
