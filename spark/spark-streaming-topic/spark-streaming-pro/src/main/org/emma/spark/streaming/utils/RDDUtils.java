package org.emma.spark.streaming.utils;

import org.emma.spark.streaming.converter.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.util.Iterator;
import java.util.Objects;

public class RDDUtils<T> {
    private static final Logger LOG = LoggerFactory.getLogger(RDDUtils.class);
    public static <T> void writeToLocalDisk(String filename, Iterator<T> rddIterator, JsonConverter<T> jsonConverter) {
        if (Objects.isNull(rddIterator) || !rddIterator.hasNext()) {
            LOG.warn("#writeToLocalDisk recv invalid iterator, return!");
        }
        try {
            FileWriter fileWriter = new FileWriter(filename, true);
            while(rddIterator.hasNext()) {
                fileWriter.write(jsonConverter.encode(rddIterator.next()));
            }
            fileWriter.close();
        } catch (Exception ex) {
            LOG.error("#writeToLocalDisk got exception", ex);
        }
    }
}
