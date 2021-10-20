package com.streamnative.cep.common.entity;

import java.math.BigDecimal;
import java.util.Map;

public class Metric {
    /** key is grouping field name, value is grouping field value*/
    private Map<String, String> tags;
    /**key is agg field name, value is the real value to agg */
    private Map<String, BigDecimal> metrics;
    /** event time*/
    private Long eventTime;

    public Metric(Map<String, String> tags,
                  Map<String, BigDecimal> metrics,
                  Long eventTime) {
        this.tags = tags;
        this.metrics = metrics;
        this.eventTime = eventTime;
    }

    public String getTag(String name) {
        return tags.get(name);
    }


    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public Map<String, BigDecimal> getMetrics() {
        return metrics;
    }

    public void setMetrics(Map<String, BigDecimal> metrics) {
        this.metrics = metrics;
    }

    public BigDecimal getMetric(String name) {
        return metrics.get(name);
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }
}
