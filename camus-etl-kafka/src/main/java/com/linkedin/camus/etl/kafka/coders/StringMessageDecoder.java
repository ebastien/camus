package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import java.util.Properties;

import java.nio.ByteBuffer;


/**
 * MessageDecoder class that will convert the payload into a String object,
 * System.currentTimeMillis() will be used to set CamusWrapper's
 * timestamp property

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
    long timestamp = 0;
    String payloadString;

    ByteBuffer buf = ByteBuffer.wrap(payloadKey);
    int len = buf.getInt();

    byte[] key = new byte[len];
    buf.get(key, 0, key.length);

    byte[] payload = new byte[buf.getInt()];
    buf.get(payload, 0, payload.length);

    key = (new String(key) + " " +  new String(payload) ).getBytes();

    payloadString = new String(key);

    timestamp = System.currentTimeMillis();

    return new CamusWrapper<String>(payloadString, timestamp);
  }
}