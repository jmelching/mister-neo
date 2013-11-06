package org.jmelching.misterneo.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AdjacencyListRelationshipReducer extends Reducer<LongWritable, LongWritable, NullWritable, Text> {

    private Text value = new Text();

    public void reduce(final LongWritable key, final Iterable<LongWritable> values,
            final Reducer<LongWritable, LongWritable, NullWritable, Text>.Context context) throws IOException,
            InterruptedException {
        StringBuilder output = new StringBuilder();

        int nodeCount = 0;
        for (LongWritable adjacentNode : values) {
            nodeCount++;
            output.append("\t").append(adjacentNode.get());
        }
        output.insert(0, nodeCount);
        output.insert(0, key + "\t");

        value.set(output.toString());
        context.write(NullWritable.get(), value);
    }

}
