package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import java.util.Properties;

import java.nio.ByteBuffer;


/**
 * MessageDecoder class that will convert the payload into a String object,
 * System.currentTimeMillis() will be used to set CamusWrapper's * timestamp property
 * This MessageDecoder returns a CamusWrapper that works with Strings payloads,
 */
public class StringMessageDecoder extends MessageDecoder<byte[], String> {
  @Override
  public void init(Properties props, String topicName) {
    this.props = props;
    this.topicName = topicName;
  }

  @Override
  public CamusWrapper<String> decode(byte[] payloadKey) {
    long timestamp = System.currentTimeMillis();
    String payloadString = new String(payloadKey);
    return new CamusWrapper<String>(payloadString, timestamp);
  }
}