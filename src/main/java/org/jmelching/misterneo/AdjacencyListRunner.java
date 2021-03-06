package org.jmelching.misterneo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jmelching.misterneo.formats.RelationshipInputFormat;
import org.jmelching.misterneo.mapreduce.RelationshipNodeMapper;
import org.jmelching.misterneo.mapreduce.AdjacencyListRelationshipReducer;

public class AdjacencyListRunner extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        
        new GenericOptionsParser(args);

        Job job = new Job(this.getConf());
        job.setJobName("adjacencyList");
        job.setJarByClass(AdjacencyListRunner.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapperClass(RelationshipNodeMapper.class);
        job.setReducerClass(AdjacencyListRelationshipReducer.class);
        job.setInputFormatClass(RelationshipInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(getConf().get("input")));
        FileOutputFormat.setOutputPath(job, new Path(getConf().get("output")));
        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new AdjacencyListRunner(), args);
        System.exit(res);
    }

}
