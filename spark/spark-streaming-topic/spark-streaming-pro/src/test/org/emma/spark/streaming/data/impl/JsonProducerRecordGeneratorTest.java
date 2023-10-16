package org.emma.spark.streaming.data.impl;


import com.clearspring.analytics.util.Lists;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.emma.spark.streaming.data.JsonProducerRecord;
import org.emma.spark.streaming.kafka.KafkaHashPartitioner;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.Objects;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

class JsonProducerRecordGeneratorTest {
    private String mockTopic = "mock_topic_name";
    private Cluster cluster;
    
    @BeforeEach
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.cluster = Mockito.mock(Cluster.class);
        when(cluster.partitionsForTopic(anyString())).thenReturn(genMockPartitionInfo(3));
    }

    @Test
    public void test_cluster_init_ok() {
        Assertions.assertTrue(Objects.nonNull(cluster));
    }

    @Test
    public void test_generate_json_message_record() {
        JsonProducerRecordGenerator generator = new JsonProducerRecordGenerator(this.mockTopic,
                new KafkaHashPartitioner(), this.cluster);
        Assertions.assertNotNull(generator);
        JsonProducerRecord record = generator.generate(System.currentTimeMillis());
        Assertions.assertNotNull(record);

        // validate each fields in the records
        Assertions.assertTrue(Objects.nonNull(record.key()) && !record.key().isEmpty());
        Assertions.assertTrue(Objects.nonNull(record.value()) && record.value().isJsonObject());
        Assertions.assertTrue(record.partition() >= 0);
    }

    private List<PartitionInfo> genMockPartitionInfo(int len) {
        List<PartitionInfo> partitionInfoList = Lists.newArrayList();

        for (int i = 0; i < len; i++) {
            PartitionInfo partitionInfo = new PartitionInfo(mockTopic, i, null, null, null);
            partitionInfoList.add(partitionInfo);
        }

        return partitionInfoList;
    }
}