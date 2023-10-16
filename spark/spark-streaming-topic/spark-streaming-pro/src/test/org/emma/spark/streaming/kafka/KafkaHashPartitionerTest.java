package org.emma.spark.streaming.kafka;

import com.clearspring.analytics.util.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

class KafkaHashPartitionerTest {
    private String mockTopic = "mock_topic_name";

    private Cluster cluster;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.initMocks(this);
        cluster = Mockito.mock(Cluster.class);
        when(cluster.partitionsForTopic(anyString())).thenReturn(genMockPartitionInfoList(3));
    }

    @Test
    public void test_customized_kafka_hash_partitioner_with_mockito() {
        Assertions.assertNotNull(cluster);
        KafkaHashPartitioner kafkaHashPartitioner = new KafkaHashPartitioner();
        Set<Integer> partitionIdSet = cluster.partitionsForTopic(mockTopic).stream().map(item -> item.partition()).collect(Collectors.toSet());
        Assertions.assertTrue(Objects.nonNull(partitionIdSet) && partitionIdSet.size() == 3);
        Set<Integer> partitionerGenPidSet = Sets.newHashSet();

        int cnt = 1;
        for (int i = 0; i < cnt; i++) {
            long start_TS = System.currentTimeMillis();
            String key = UUID.randomUUID().toString();
            String value = UUID.randomUUID().toString();
            int partitionId = kafkaHashPartitioner.partition(mockTopic, key, key.getBytes(), value, value.getBytes(), cluster);
            partitionIdSet.add(partitionId);
            long end_TS = System.currentTimeMillis();
            System.out.println("start_ts: " + start_TS + ", end_ts:" + end_TS + ", total cost " + (end_TS - start_TS));
        }

        Assertions.assertTrue(partitionIdSet.containsAll(partitionerGenPidSet));
    }

    private List<PartitionInfo> genMockPartitionInfoList(int len) {
        List<PartitionInfo> partitionInfoList = Lists.newArrayList();

        for (int i = 0; i < len; i++) {
            PartitionInfo partitionInfo = new PartitionInfo(mockTopic, i, null, null, null);
            partitionInfoList.add(partitionInfo);
        }

        return partitionInfoList;
    }
}