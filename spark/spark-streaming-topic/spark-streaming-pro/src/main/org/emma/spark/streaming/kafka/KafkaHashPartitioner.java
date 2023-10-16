package org.emma.spark.streaming.kafka;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.spark.util.CollectionsUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class KafkaHashPartitioner implements Partitioner {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaHashPartitioner.class);
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Assert.assertNotNull(cluster);
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        List<PartitionInfo> availablePartitions = cluster.partitionsForTopic(topic);

        Assert.assertTrue(CollectionUtils.isNotEmpty(partitions) && CollectionUtils.isNotEmpty(availablePartitions));
        List<Integer> partitionIdSet = null;

        if (CollectionUtils.isEqualCollection(partitions, availablePartitions)) {
            partitionIdSet = partitions.stream().map(item -> item.partition()).collect(Collectors.toList());
        } else {
            LOG.info("#partition valid partition total cnt {}", partitionIdSet.size());
            partitionIdSet = availablePartitions.stream().map(item -> item.partition()).collect(Collectors.toList());
        }

        int partitionId = -1;

        while (!partitionIdSet.contains(partitionId)) {
            int totalNumPartitions = partitions.size();
            partitionId = key.hashCode() % totalNumPartitions;
        }

        LOG.info("#partition got partition id {}", partitionId);
        return partitionId;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
