package org.jmelching.misterneo.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.jmelching.misterneo.mapreduce.AdjacencyListRelationshipMapper;
import org.jmelching.misterneo.writables.RelationshipRecordWritable;
import org.junit.Before;
import org.junit.Test;

public class AdjacencyListRelationshipMapperTest {

    MapDriver<NullWritable, RelationshipRecordWritable, LongWritable, LongWritable> mapDriver;

    @Before
    public void setUp() {
        AdjacencyListRelationshipMapper mapper = new AdjacencyListRelationshipMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(NullWritable.get(), new RelationshipRecordWritable(1, 2, 0));
        mapDriver.withInput(NullWritable.get(), new RelationshipRecordWritable(1, 3, 0));
        mapDriver.withInput(NullWritable.get(), new RelationshipRecordWritable(3, 2, 1));
        mapDriver.withOutput(new LongWritable(1), new LongWritable(2));
        mapDriver.withOutput(new LongWritable(1), new LongWritable(3));
        mapDriver.withOutput(new LongWritable(3), new LongWritable(2));
        mapDriver.runTest();
    }

}
