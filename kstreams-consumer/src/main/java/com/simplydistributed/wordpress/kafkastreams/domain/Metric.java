package com.simplydistributed.wordpress.kafkastreams.domain;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * POJO representing a single CPU metric
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class Metric {
    private String machine;
    private String cpu;
    private String source;

    public Metric() {
    }

    public Metric(String source,String machine, String cpu) {
        this.machine = machine;
        this.cpu = cpu;
        this.source = source;
    }

    @Override
    public String toString() {
        return "Metric{" + "machine=" + machine + ", cpu=" + cpu + ", source=" + source + '}';
    }

    
    
}
