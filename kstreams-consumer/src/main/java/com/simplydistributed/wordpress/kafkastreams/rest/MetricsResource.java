package com.simplydistributed.wordpress.kafkastreams.rest;

import com.simplydistributed.wordpress.kafkastreams.CPUMetricStreamHandler;
import com.simplydistributed.wordpress.kafkastreams.GlobalAppState;
import com.simplydistributed.wordpress.kafkastreams.Utils;
import com.simplydistributed.wordpress.kafkastreams.domain.Metrics;
import java.util.concurrent.TimeUnit;

import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.kafka.common.serialization.StringSerializer;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;

/**
 * REST interface for fetching Kafka Stream processing state. Exposes endpoints
 * for local as well as remote client
 */
@Path("metrics")
public final class MetricsResource {

    private final String storeName;

    public MetricsResource() {
        storeName = CPUMetricStreamHandler.AVG_STORE_NAME;
    }

    private static final Logger LOGGER = Logger.getLogger(MetricsResource.class.getName());

    /**
     * Local interface for fetching metrics
     *
     * @return Metrics from local and other remote stores (if needed)
     * @throws Exception
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Response all_metrics() throws Exception {
        Response response = null;
        try {
            KafkaStreams ks = GlobalAppState.getInstance().getKafkaStreams();
            HostInfo thisInstance = GlobalAppState.getInstance().getHostPortInfo();

            LOGGER.info("Querying local store");
            Metrics metrics = getLocalMetrics();

            // LOGGER.info("Querying remote stores....");
            ks.allMetadataForStore(storeName)
                    .stream()
                    .filter(sm -> !(sm.host().equals(thisInstance.host()) && sm.port() == thisInstance.port())) //only query remote node stores
                    .forEach(new Consumer<StreamsMetadata>() {
                        @Override
                        public void accept(StreamsMetadata t) {
                            String url = "http://" + t.host() + ":" + t.port() + "/metrics/remote";
                            //LOGGER.log(Level.INFO, "Fetching remote store at {0}", url);
                            Metrics remoteMetrics = Utils.getRemoteStoreState(url, 2, TimeUnit.SECONDS);
                            metrics.add(remoteMetrics);
                            LOGGER.log(Level.INFO, "Metric from remote store at {0} == {1}", new Object[]{url, remoteMetrics});
                        }

                    });

            LOGGER.log(Level.INFO, "Complete store state {0}", metrics);
            response = Response.ok(metrics).build();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error - {0}", e.getMessage());
            e.printStackTrace();
        }

        return response;
    }

    /**
     * Remote interface for fetching metrics
     *
     * @return Metrics
     */
    @GET
    @Path("remote")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Metrics remote() {
        LOGGER.info("Remote interface invoked");
        Metrics metrics = null;
        try {
            metrics = getLocalMetrics();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error - {0}", e.getMessage());
            e.printStackTrace();
        }

        return metrics;

    }

    /**
     * Query local state store to extract metrics
     *
     * @return local Metrics
     */
    private Metrics getLocalMetrics() {
        HostInfo thisInstance = GlobalAppState.getInstance().getHostPortInfo();
        KafkaStreams ks = GlobalAppState.getInstance().getKafkaStreams();

        String source = thisInstance.host() + ":" + thisInstance.port();
        Metrics localMetrics = new Metrics();

        ReadOnlyKeyValueStore<String, Double> averageStore = ks
                .store(storeName,
                        QueryableStoreTypes.<String, Double>keyValueStore());

        LOGGER.log(Level.INFO, "Entries in store {0}", averageStore.approximateNumEntries());
        KeyValueIterator<String, Double> storeIterator = averageStore.all();

        while (storeIterator.hasNext()) {
            KeyValue<String, Double> kv = storeIterator.next();
            localMetrics.add(source, kv.key, String.valueOf(kv.value));

        }
        LOGGER.log(Level.INFO, "Local store state {0}", localMetrics);
        return localMetrics;
    }

    /**
     * Metrics for a machine
     *
     * @param machine
     * @return the metric
     */
    @GET
    @Path("{machine}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Response getMachineMetric(@PathParam("machine") String machine) {
        LOGGER.log(Level.INFO, "Fetching metrics for machine {0}", machine);

        KafkaStreams ks = GlobalAppState.getInstance().getKafkaStreams();
        HostInfo thisInstance = GlobalAppState.getInstance().getHostPortInfo();

        Metrics metrics = null;

        StreamsMetadata metadataForMachine = ks.metadataForKey(storeName, machine, new StringSerializer());

        if (metadataForMachine.host().equals(thisInstance.host()) && metadataForMachine.port() == thisInstance.port()) {
            LOGGER.log(Level.INFO, "Querying local store for machine {0}", machine);
            metrics = getLocalMetrics(machine);
        } else {
            //LOGGER.log(Level.INFO, "Querying remote store for machine {0}", machine);
            String url = "http://" + metadataForMachine.host() + ":" + metadataForMachine.port() + "/metrics/remote/" + machine;
            metrics = Utils.getRemoteStoreState(url, 2, TimeUnit.SECONDS);
            LOGGER.log(Level.INFO, "Metric from remote store at {0} == {1}", new Object[]{url, metrics});
        }

        return Response.ok(metrics).build();
    }

    /**
     * Remote interface for fetching a specific machine's metric
     * @param machine
     * @return Metrics
     */
    @GET
    @Path("remote/{machine}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Metrics remote(@PathParam("machine") String machine) {
        LOGGER.log(Level.INFO, "Remote interface invoked for machine {0}", machine);
        Metrics metrics = null;
        try {
            metrics = getLocalMetrics(machine);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error - {0}", e.getMessage());
            e.printStackTrace();
        }

        return metrics;

    }

    /**
     * get Metrics for a machine
     * @param machine
     * @return 
     */
    private Metrics getLocalMetrics(String machine) {
        LOGGER.log(Level.INFO, "Getting Metrics for machine {0}", machine);
        
        HostInfo thisInstance = GlobalAppState.getInstance().getHostPortInfo();
        KafkaStreams ks = GlobalAppState.getInstance().getKafkaStreams();

        String source = thisInstance.host() + ":" + thisInstance.port();
        Metrics localMetrics = new Metrics();

        ReadOnlyKeyValueStore<String, Double> averageStore = ks
                .store(storeName,
                        QueryableStoreTypes.<String, Double>keyValueStore());

        LOGGER.log(Level.INFO, "Entries in store {0}", averageStore.approximateNumEntries());

        localMetrics.add(source, machine, String.valueOf(averageStore.get(machine)));

        LOGGER.log(Level.INFO, "Metrics for machine {0} - {1}", new Object[]{machine, localMetrics});
        return localMetrics;
    }

}
