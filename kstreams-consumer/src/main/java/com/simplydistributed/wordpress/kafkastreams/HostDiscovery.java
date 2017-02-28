package com.simplydistributed.wordpress.kafkastreams;

/**
 * in case you want to plugin some custom way of discovering your application address
 */
public interface HostDiscovery {
    public String getHost();
    
    /**
     * default
     */
    public static final HostDiscovery defaultLocalHostDiscovery = () -> "localhost";
}
