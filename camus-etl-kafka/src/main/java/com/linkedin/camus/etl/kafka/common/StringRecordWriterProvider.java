package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.avro.file.CodecFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import org.apache.commons.codec.binary.Base64;


/**
 * Provides a RecordWriter that uses FSDataOutputStream to write
 * a String record as bytes to HDFS without any reformatting or compression.
 */
public class StringRecordWriterProvider implements RecordWriterProvider {
  public static final String ETL_OUTPUT_RECORD_DELIMITER = "etl.output.record.delimiter";
  public static final String DEFAULT_RECORD_DELIMITER = "\n";
  public static final String DUMP_KEY = "string.record.writer.dumpkey";


  protected String recordDelimiter = null;

  private String extension = "";
  private boolean isCompressed = false;
  private CompressionCodec codec = null;

  public StringRecordWriterProvider(TaskAttemptContext context) {
    Configuration conf = context.getConfiguration();

    if (recordDelimiter == null) {
      recordDelimiter = conf.get(ETL_OUTPUT_RECORD_DELIMITER, DEFAULT_RECORD_DELIMITER);
    }

    isCompressed = FileOutputFormat.getCompressOutput(context);

    if (isCompressed) {
      Class<? extends CompressionCodec> codecClass = null;
      if ("snappy".equals(EtlMultiOutputFormat.getEtlOutputCodec(context))) {
        codecClass = SnappyCodec.class;
      } else if ("gzip".equals((EtlMultiOutputFormat.getEtlOutputCodec(context)))) {
        codecClass = GzipCodec.class;
      } else {
        codecClass = DefaultCodec.class;
      }
      codec = ReflectionUtils.newInstance(codecClass, conf);
      extension = codec.getDefaultExtension();
    }
  }

  // TODO: Make this configurable somehow.
  // To do this, we'd have to make RecordWriterProvider have an
  // init(JobContext context) method signature that EtlMultiOutputFormat would always call.
  @Override
  public String getFilenameExtension() {
    return extension;
  }

  @Override
  public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(TaskAttemptContext context, String fileName,
      CamusWrapper camusWrapper, FileOutputCommitter committer) throws IOException, InterruptedException {

    // If recordDelimiter hasn't been initialized, do so now
    if (recordDelimiter == null) {
      recordDelimiter = context.getConfiguration().get(ETL_OUTPUT_RECORD_DELIMITER, DEFAULT_RECORD_DELIMITER);
    }

    // Get the filename for this RecordWriter.
    Path path =
        new Path(committer.getWorkPath(), EtlMultiOutputFormat.getUniqueFile(context, fileName, getFilenameExtension()));

    FileSystem fs = path.getFileSystem(context.getConfiguration());
    if (!isCompressed) {
      FSDataOutputStream fileOut = fs.create(path, false);
      return new ByteRecordWriter(fileOut, recordDelimiter, context.getConfiguration().getBoolean(DUMP_KEY, false));
    } else {
      FSDataOutputStream fileOut = fs.create(path, false);
      return new ByteRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), recordDelimiter,
                                    context.getConfiguration().getBoolean(DUMP_KEY, false));
    }

  }

  protected static class ByteRecordWriter extends RecordWriter<IEtlKey, CamusWrapper> {
    private DataOutputStream out;
    private String recordDelimiter;
    private boolean dumpKey;

    public ByteRecordWriter(DataOutputStream out, String recordDelimiter, boolean dumpKey) {
      this.out = out;
      this.recordDelimiter = recordDelimiter;
      this.dumpKey = dumpKey;
    }

    @Override
    public void write(IEtlKey ignore, CamusWrapper value) throws IOException {
      boolean nullValue = value == null;
      if (!nullValue) {
        String record = (String) value.getRecord();
        String[] keyValueSplit = record.split(" ", -1);
        if (keyValueSplit.length == 2) {
          Base64 base64 = new Base64();
          byte[] decoded = null;
          try {
            decoded = base64.decodeBase64(keyValueSplit[1].getBytes());
          }
          catch (Exception e) {
            throw new IOException(e);
          }
          if (decoded != null) {
            if (dumpKey) {
              out.write(keyValueSplit[0].getBytes());
              out.write(" ".getBytes());
            }
            out.write(decoded);
            out.write(recordDelimiter.getBytes());
          }
        }
      }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
      out.close();
    }
  }
}
