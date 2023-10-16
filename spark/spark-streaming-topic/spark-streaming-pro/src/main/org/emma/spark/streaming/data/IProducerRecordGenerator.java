package org.emma.spark.streaming.data;

import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface IProducerRecordGenerator<K, V> {
    ProducerRecord<K, V> generate(Long timestamp);

    K genRandomKey();

    V genRandomValue();
}
