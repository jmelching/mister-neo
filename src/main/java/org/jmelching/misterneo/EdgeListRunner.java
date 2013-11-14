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

public class EdgeListRunner extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        
        new GenericOptionsParser(args);
        

        Job job = new Job(this.getConf());
        job.setJobName("edgeList");
        job.setJarByClass(EdgeListRunner.class);
        job.setInputFormatClass(RelationshipInputFormat.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapperClass(RelationshipNodeMapper.class);
        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job, new Path(getConf().get("input")));
        FileOutputFormat.setOutputPath(job, new Path(getConf().get("output")));
        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new EdgeListRunner(), args);
        System.exit(res);
    }

}
