package org.emma.spark.streaming.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.emma.spark.streaming.data.IProducerRecordGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaMessageProducer<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageProducer.class);

    private String brokerEndpoint;
    //"localhost:9092,localhost:9093,localhost:9094";
    private String topic;
    private Properties kafkaProperties;
    private KafkaProducer<K, V> kafkaProducer;

    private IProducerRecordGenerator<K, V> recordGenerator;

    public KafkaMessageProducer(String brokerEndpoint, String topic,
                                Properties kafkaProperties, IProducerRecordGenerator recordGenerator) {
        this.brokerEndpoint = brokerEndpoint;
        this.topic = topic;
        this.kafkaProperties = kafkaProperties;
        this.kafkaProducer = new KafkaProducer<>(kafkaProperties);
        this.recordGenerator = recordGenerator;
    }

    public void produceMessage() {
        try {
            ProducerRecord<K, V> message = recordGenerator.generate(System.currentTimeMillis());
            Future<RecordMetadata> ret = kafkaProducer.send(message);
            LOG.info("#produceMessage message sent to topic {}, partition no. {},  \n" +
                            "with key serialization len {}, with value serialization len {}, hasOffset {}, offset value {}",
                    ret.get().topic(), ret.get().partition(),
                    ret.get().serializedKeySize(), ret.get().serializedValueSize(),
                    ret.get().hasOffset(), ret.get().offset());
        } catch (Exception ex) {
            LOG.error("#produceMessage got exception!", ex);
        }
    }
}
