package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import java.util.Properties;

/**
 * BytesMessageDecoder class that will convert keep the payload as an array of bytes,
 * System.currentTimeMillis() will be used to set CamusWrapper's * timestamp property
 * This BytesMessageDecoder returns a CamusWrapper that works with bytes payloads,
 */
public class BytesMessageDecoder extends MessageDecoder<byte[], byte[]> {
  @Override
  public void init(Properties props, String topicName) {
    this.props = props;
    this.topicName = topicName;
  }

  @Override
  public CamusWrapper<byte[]> decode(byte[] payload) {
    long timestamp = System.currentTimeMillis();
    return new CamusWrapper<byte[]>(payload, timestamp);
  }
}
