package org.emma.spark.streaming.data.impl;

import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.emma.spark.streaming.data.IProducerRecordGenerator;
import org.emma.spark.streaming.data.JsonProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class JsonProducerRecordGenerator implements IProducerRecordGenerator<String, JsonObject> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonProducerRecordGenerator.class);

    private String topic;
    private Partitioner partitioner;
    private AtomicLong globalId;
    private Cluster cluster;

    public JsonProducerRecordGenerator(String topic, Partitioner partitioner, Cluster cluster) {
        this.topic = topic;
        this.partitioner = partitioner;
        this.globalId = new AtomicLong(0L);
        this.cluster = cluster;
    }

    @Override
    public JsonProducerRecord generate(Long timestamp) {
        String key = genRandomKey();
        JsonObject value = genRandomValue();
        int partition = getPartitionId(key, value);
        JsonProducerRecord ret = new JsonProducerRecord(this.topic, partition, timestamp, key, value);
        return ret;
    }

    private int getPartitionId(String key, JsonObject value) {
        int partitionId = partitioner.partition(this.topic, key, key.getBytes(), value, null, this.cluster);
        return partitionId;
    }

    @Override
    public JsonObject genRandomValue() {
        JsonObject ret = new JsonObject();
        ret.addProperty("name", UUID.randomUUID().toString());
        ret.addProperty("address", UUID.randomUUID().toString());
        ret.addProperty("content", UUID.randomUUID().toString());
        ret.addProperty("globalId", this.globalId.getAndAdd(1));
        return ret;
    }

    @Override
    public String genRandomKey() {
        return UUID.randomUUID().toString();
    }
}
