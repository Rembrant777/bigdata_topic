package org.emma.spark.streaming.data;

import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

public class JsonProducerRecord extends ProducerRecord<String, JsonObject> {
    public JsonProducerRecord(String topic, Integer partition, Long timestamp,
                              String key, JsonObject value) {
        super(topic, partition, timestamp, key, value, null);
    }
}
