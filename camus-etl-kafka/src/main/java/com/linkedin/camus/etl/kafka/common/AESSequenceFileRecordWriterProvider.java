package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;

import org.apache.log4j.Logger;

import java.security.GeneralSecurityException;

/**
 * Provides a RecordWriter that uses SequenceFile.Writer to write
 * SequenceFiles records to HDFS.  Compression settings are controlled via
 * the usual hadoop configuration values.
 *
 * - mapreduce.output.fileoutputformat.compress         - true or false
 * - mapreduce.output.fileoutputformat.compress.codec   - org.apache.hadoop.io.compress.* (SnappyCodec, etc.)
 * - mapreduce.output.fileoutputformat.compress.type    - BLOCK or RECORD
 *
 */
public class AESSequenceFileRecordWriterProvider implements RecordWriterProvider {

  public static final String CAMUS_CRYPT_PASSPHRASE = "camus.crypt.passphrase";

  private static Logger log =
    Logger.getLogger(SequenceFileRecordWriterProvider.class);

  public AESSequenceFileRecordWriterProvider(TaskAttemptContext context) {
  }

  // TODO: Make this configurable somehow.
  // To do this, we'd have to make SequenceFileRecordWriterProvider have an
  // init(JobContext context) method signature that EtlMultiOutputFormat would always call.
  @Override
  public String getFilenameExtension() {
    return "";
  }

  @Override
  public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(
        TaskAttemptContext context,
        String fileName,
        CamusWrapper camusWrapper,
        FileOutputCommitter committer
      ) throws IOException, InterruptedException {

    final Configuration conf = context.getConfiguration();

    CompressionCodec compressionCodec = null;
    CompressionType compressionType = CompressionType.NONE;

    // Determine compression type (BLOCK or RECORD) and compression codec to use.
    if (SequenceFileOutputFormat.getCompressOutput(context)) {
      compressionType = SequenceFileOutputFormat.getOutputCompressionType(context);
      Class<?> codecClass = SequenceFileOutputFormat.getOutputCompressorClass(
                              context,
                              DefaultCodec.class
                            );
      // Instantiate the CompressionCodec Class
      compressionCodec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
    }

    // Get the filename for this RecordWriter.
    Path path = new Path(
                  committer.getWorkPath(),
                  EtlMultiOutputFormat.getUniqueFile(
                    context, fileName, getFilenameExtension()
                  )
                );

    log.info(
        "Creating new SequenceFile.Writer with compression type " +
        compressionType + " and compression codec " + (
          compressionCodec != null ? compressionCodec.getClass().getName() : "null"
        )
      );

    final SequenceFile.Writer writer =
        SequenceFile.createWriter(
          path.getFileSystem(conf), conf, path,
          Text.class, // Key as Text but always empty
          BytesWritable.class, // We are writing sequences of bytes
          compressionType, compressionCodec, context
        );

    final String passphrase = conf.get(CAMUS_CRYPT_PASSPHRASE);

    if (passphrase == null) {
      throw new InterruptedException("missing passphrase");
    }

    // Return a new anonymous RecordWriter that uses the
    // SequenceFile.Writer to write data to HDFS
    return new RecordWriter<IEtlKey, CamusWrapper>() {

      // We will always write an empty key
      private final Text emptyKey = new Text();

      private AESCodec codec = new AESCodec(passphrase.getBytes("UTF-8"));

      @Override
      public void write(IEtlKey key, CamusWrapper data)
          throws IOException, InterruptedException {

        byte[] record = (byte[]) data.getRecord();
        BytesWritable message = null;

        try {
          message = new BytesWritable(codec.encrypt(record));
        } catch (GeneralSecurityException e) {
          throw new IOException(e.getMessage());
        }

        writer.append(emptyKey, message);
      }

      @Override
      public void close(TaskAttemptContext context)
          throws IOException, InterruptedException {
        writer.close();
      }
    };
  }
}
