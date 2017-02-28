package com.simplydistributed.wordpress.kafkastreams.domain;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * POJO representing list of Metric(s)
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class Metrics {

    private final List<Metric> metrics;

    public Metrics() {
        metrics = new ArrayList<>();
    }

    public Metrics add(String source, String machine, String cpu) {
        metrics.add(new Metric(source, machine, cpu));
        return this;
    }

    public Metrics add(Metrics anotherMetrics) {
        anotherMetrics.metrics.forEach((metric) -> {
            metrics.add(metric);
        });
        return this;
    }

    @Override
    public String toString() {
        return "Metrics{" + "metrics=" + metrics + '}';
    }
    
    public static Metrics EMPTY(){
        return new Metrics();
    }
    
}
