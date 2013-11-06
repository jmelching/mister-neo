package org.jmelching.misterneo.formats;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.jmelching.misterneo.writables.RelationshipRecordWritable;

/**
 * 
 * @author jmelching at github.com
 * 
 */
public class RelationshipInputFormat extends FileInputFormat<NullWritable, RelationshipRecordWritable> {

    @Override
    public RecordReader<NullWritable, RelationshipRecordWritable> createRecordReader(final InputSplit split,
            final TaskAttemptContext context) {
        return new RelationshipRecordReader();
    }

    @Override
    protected boolean isSplitable(final JobContext context, final Path file) {
        // return null == new
        // CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return true;
    }

}