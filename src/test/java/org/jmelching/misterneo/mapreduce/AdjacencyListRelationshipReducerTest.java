package org.jmelching.misterneo.mapreduce;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.jmelching.misterneo.mapreduce.AdjacencyListRelationshipReducer;
import org.junit.Before;
import org.junit.Test;

public class AdjacencyListRelationshipReducerTest {

    ReduceDriver<LongWritable, LongWritable, NullWritable, Text> reduceDriver;

    @Before
    public void setUp() {
        AdjacencyListRelationshipReducer reducer = new AdjacencyListRelationshipReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testMapper() throws IOException {
        reduceDriver.withInput(new LongWritable(1), Arrays.asList(new LongWritable(3), new LongWritable(2)))
                .withInput(new LongWritable(2), Arrays.asList(new LongWritable(4), new LongWritable(3)))
                .withOutput(NullWritable.get(), new Text("1\t2\t3\t2"))
                .withOutput(NullWritable.get(), new Text("2\t2\t4\t3"));
        reduceDriver.runTest();
    }

}
