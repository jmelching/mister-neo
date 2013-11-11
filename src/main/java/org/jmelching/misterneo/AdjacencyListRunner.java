package org.jmelching.misterneo;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jmelching.misterneo.formats.RelationshipInputFormat;
import org.jmelching.misterneo.mapreduce.AdjacencyListRelationshipMapper;
import org.jmelching.misterneo.mapreduce.AdjacencyListRelationshipReducer;

public class AdjacencyListRunner extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        
//        getConf().set("mapred.max.split.size","156");

        Job job = new Job(this.getConf());
        job.setJobName("adjacencyList");
        job.setJarByClass(AdjacencyListRunner.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapperClass(AdjacencyListRelationshipMapper.class);
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
