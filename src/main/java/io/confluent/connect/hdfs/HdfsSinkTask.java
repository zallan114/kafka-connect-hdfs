/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.hdfs;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.schema.StorageSchemaCompatibility;
import java.io.OutputStream;

public class HdfsSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(HdfsSinkTask.class);
  private DataWriter hdfsWriter;
  private AvroData avroData;
  //HdfsSinkConnectorConfig connectorConfig;

  public HdfsSinkTask() {}

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    Set<TopicPartition> assignment = context.assignment();
    try {
      HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);
      boolean hiveIntegration = connectorConfig.getBoolean(HiveConfig.HIVE_INTEGRATION_CONFIG);
      if (hiveIntegration) {
        StorageSchemaCompatibility compatibility = StorageSchemaCompatibility.getCompatibility(
            connectorConfig.getString(HiveConfig.SCHEMA_COMPATIBILITY_CONFIG)
        );
        if (compatibility == StorageSchemaCompatibility.NONE) {
          throw new ConfigException(
              "Hive Integration requires schema compatibility to be BACKWARD, FORWARD or FULL"
          );
        }
      }

      //check that timezone it setup correctly in case of scheduled rotation
      if (connectorConfig.getLong(HdfsSinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG) > 0) {
        String timeZoneString = connectorConfig.getString(PartitionerConfig.TIMEZONE_CONFIG);
        if (timeZoneString.equals("")) {
          throw new ConfigException(PartitionerConfig.TIMEZONE_CONFIG,
              timeZoneString, "Timezone cannot be empty when using scheduled file rotation."
          );
        }
        DateTimeZone.forID(timeZoneString);
      }

      int schemaCacheSize = connectorConfig.getInt(
          HdfsSinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG
      );
      avroData = new AvroData(schemaCacheSize);
      hdfsWriter = new DataWriter(connectorConfig, context, avroData);
      recover(assignment);
      if (hiveIntegration) {
        syncWithHive();
      }
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start HdfsSinkConnector due to configuration error.", e);
    } catch (ConnectException e) {
      log.info("Couldn't start HdfsSinkConnector:", e);
      log.info("Shutting down HdfsSinkConnector.");
      if (hdfsWriter != null) {
        hdfsWriter.close();
        hdfsWriter.stop();
      }
    }

    log.info("The connector relies on offsets in HDFS filenames, but does commit these offsets to "
        + "Connect to enable monitoring progress of the HDFS connector. Upon startup, the HDFS "
        + "Connector restores offsets from filenames in HDFS. In the absence of files in HDFS, "
        + "the connector will attempt to find offsets for its consumer group in the "
        + "'__consumer_offsets' topic. If offsets are not found, the consumer will "
        + "rely on the reset policy specified in the 'consumer.auto.offset.reset' property to "
        + "start exporting data to HDFS.");
  }

  @Override
  public void put(Collection<SinkRecord> records) throws ConnectException {
    if (log.isDebugEnabled()) {
      log.debug("Read {} records from Kafka", records.size());
    }    
    
    Collection<SinkRecord> validRecords = new ArrayList<SinkRecord>();
    for (SinkRecord record : records) {
      String dataStr = record.value().toString();
      log.debug("write info***" + dataStr + "***");
      if (dataStr.indexOf("cas_wrong_record") < 0) {
        validRecords.add(record);
      } else {
        logInvalidRecords(record);
      }
    }
    
    try {
      hdfsWriter.write(validRecords);
    } catch (ConnectException e) {
      throw new ConnectException(e);
    }
  }
  
  private void logInvalidRecords(SinkRecord record) {

    String logPath = "/logs/" + record.topic() 
        + "/pos_" + record.kafkaPartition() + "_" + record.kafkaOffset() + ".txt";

    HdfsStorage storage = (HdfsStorage)hdfsWriter.getStorage();
    
    OutputStream os = storage.create(logPath, true);

    try {

      os.write((record.value().toString()).getBytes("UTF-8"));
      os.flush();
      
      log.info("***logInvalidRecords done({})***", logPath);  
    } catch (Exception e) {
      e.toString();
    } finally {
      try {
        os.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    
  }
  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets
  ) {
    // Although the connector manages offsets via files in HDFS, we still want to have Connect
    // commit the consumer offsets for records this task has consumed from its topic partitions and
    // committed to HDFS.
    Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
    for (Map.Entry<TopicPartition, Long> entry : hdfsWriter.getCommittedOffsets().entrySet()) {
      log.debug(
          "Found last committed offset {} for topic partition {}",
          entry.getValue(),
          entry.getKey()
      );
      result.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
    }
    log.debug("Returning committed offsets {}", result);
    return result;
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    hdfsWriter.open(partitions);
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    if (hdfsWriter != null) {
      hdfsWriter.close();
    }
  }

  @Override
  public void stop() throws ConnectException {
    if (hdfsWriter != null) {
      hdfsWriter.stop();
    }
  }

  private void recover(Set<TopicPartition> assignment) {
    for (TopicPartition tp : assignment) {
      hdfsWriter.recover(tp);
    }
  }

  private void syncWithHive() throws ConnectException {
    hdfsWriter.syncWithHive();
  }

  public AvroData getAvroData() {
    return avroData;
  }
}
