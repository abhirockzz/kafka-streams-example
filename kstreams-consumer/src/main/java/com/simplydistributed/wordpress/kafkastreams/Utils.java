package com.simplydistributed.wordpress.kafkastreams;

import com.simplydistributed.wordpress.kafkastreams.domain.Metrics;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;

/**
 * Common utilites
 */
public final class Utils {

    private static final Logger LOGGER = Logger.getLogger(Utils.class.getSimpleName());

    private Utils() {
    }

    private static Client REST_CLIENT = null;

    private static Client getRESTClient() {
        if (REST_CLIENT == null) {
            REST_CLIENT = ClientBuilder.newClient();
        }
        return REST_CLIENT;
    }

    /**
     * Fetch metrics using REST call
     * 
     * @param url remote metric endpoint URL
     * @param timeout duration for which to wait for response before giving up
     * @param unit the unit (secs, ms etc.)
     * 
     * @return the Metrics
     */
    public static Metrics getRemoteStoreState(String url, long timeout, TimeUnit unit) {
        Metrics metrics = null;
        try {
            Future<Metrics> metricsF = getRESTClient().target(url)
                    .request(MediaType.APPLICATION_JSON)
                    .async() //returns asap
                    .get(Metrics.class);
            
            metrics = metricsF.get(timeout, unit); //blocks until timeout
            
        } catch (Exception ex) {
            LOGGER.log(Level.WARNING, "Metrics fetch from {0} timed out", url);
            metrics = Metrics.EMPTY();
        }

        return metrics;
    }

    public static Metrics getRemoteStoreState(String url) {

        Metrics response = getRESTClient().target(url)
                .request(MediaType.APPLICATION_JSON)
                .get(Metrics.class);
        return response;
    }

    public static void closeRESTClient() {
        if (REST_CLIENT != null) {
            REST_CLIENT.close();
            LOGGER.info("REST Client closed");
        }
    }

    public static String getHostIPForDiscovery() {
        //implement a new mechanism for host discovery if needed and use it here
        String host = HostDiscovery.defaultLocalHostDiscovery.getHost();
        
        LOGGER.log(Level.INFO, " Host IP {0}", host);
        return host;
    }
}
