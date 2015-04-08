package com.linkedin.camus.etl.kafka.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Map.Entry;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import com.linkedin.camus.coders.MessageEncoder;
import com.linkedin.camus.etl.kafka.CamusJob;


public class MyEtlCounts extends EtlCounts {

  private static Logger log = Logger.getLogger(EtlCounts.class);



  public MyEtlCounts(EtlCounts other) {
    super(other);
  }

  @Override
  public void postTrackingCountToKafka(Configuration conf, String tier, String brokerList) {
    //("post tracking count");
    /*MessageEncoder<IndexedRecord, byte[]> encoder;
    AbstractMonitoringEvent monitoringDetails;
    try {
      encoder =
          (MessageEncoder<IndexedRecord, byte[]>) Class.forName(conf.get(CamusJob.CAMUS_MESSAGE_ENCODER_CLASS))
              .newInstance();

      Properties props = new Properties();
      for (Entry<String, String> entry : conf) {
        props.put(entry.getKey(), entry.getValue());
      }

      encoder.init(props, "TrackingMonitoringEvent");
      monitoringDetails =
          (AbstractMonitoringEvent) Class.forName(getMonitoringEventClass(conf))
          .getDeclaredConstructor(Configuration.class).newInstance(conf);
    } catch (Exception e1) {
      throw new RuntimeException(e1);
    }*/

    ArrayList<byte[]> monitorSet = new ArrayList<byte[]>();
    int counts = 0;

    for (Map.Entry<String, Source> singleCount : this.getCounts().entrySet()) {
      Source countEntry = singleCount.getValue();
      counts++;
      log.info(countEntry.toString() + ", topic: " + getTopic() +", granularity " + getGranularity() + ", tier = " + tier + 
        " startTime:" + getStartTime() +
        " endTime:" + getEndTime() +
        " errorCount:" + getErrorCount() + 
        " first: " + getFirstTimestamp() + 
        " last: " + getLastTimestamp() + 
        " event count: " + getEventCount());
     /* GenericRecord monitoringRecord =
          monitoringDetails.createMonitoringEventRecord(countEntry, topic, granularity, tier);
      byte[] message = encoder.toBytes((IndexedRecord) monitoringRecord);
      monitorSet.add(message);

      if (monitorSet.size() >= 2000) {
        counts += monitorSet.size();
        produceCount(brokerList, monitorSet);
        monitorSet.clear();
      }*/
    }

    /*if (monitorSet.size() > 0) {
      counts += monitorSet.size();
      produceCount(brokerList, monitorSet);
    }*/

    log.info(getTopic() + " sent " + counts + " counts");
  }

}