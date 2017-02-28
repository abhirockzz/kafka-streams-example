package com.simplydistributed.wordpress.kafkastreams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;

/**
 * Stores global application state
 */
public final class GlobalAppState {
    private HostInfo hostInfo;
    private KafkaStreams kafkaStreams;
    
    private GlobalAppState() {
    }
    
    private static GlobalAppState INSTANCE = new GlobalAppState();
    
    public static GlobalAppState getInstance(){
        return INSTANCE;
    }
    
    public GlobalAppState hostPortInfo(String host, String port){
        hostInfo = new HostInfo(host, Integer.parseInt(port));
        return this;
    }
    
    public GlobalAppState streams(KafkaStreams ks){
        kafkaStreams = ks;
        return this;
    }
    
    public HostInfo getHostPortInfo() {
        return this.hostInfo;
    }
    
    public KafkaStreams getKafkaStreams(){
        return this.kafkaStreams;
    }
 
}
