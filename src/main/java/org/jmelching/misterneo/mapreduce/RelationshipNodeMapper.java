package org.jmelching.misterneo.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.jmelching.misterneo.writables.RelationshipRecordWritable;

public class RelationshipNodeMapper extends Mapper<NullWritable, RelationshipRecordWritable, LongWritable, LongWritable> {
    private LongWritable key = new LongWritable();
    private LongWritable value = new LongWritable();

    @Override
    protected void map(NullWritable n, RelationshipRecordWritable record, Context context) throws java.io.IOException, InterruptedException {
        key.set(record.getFirstNode());
        value.set(record.getSecondNode());
        context.write(key, value);
        
    }

}
