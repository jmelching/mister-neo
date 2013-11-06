package org.jmelching.misterneo.formats;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.jmelching.misterneo.formats.RelationshipRecordReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * 
 * @author jmelching at github.com
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class RelationshipRecordReaderTest {
    RelationshipRecordReader reader = new RelationshipRecordReader();

    @Mock
    FileSplit genericSplit;

    @Mock
    TaskAttemptContext context;

    @Before
    public void setup() throws IOException, InterruptedException {
        String path = RelationshipRecordReaderTest.class.getClassLoader()
                .getResource("mister-neo/neostore.relationshipstore.db").getPath();

        when(genericSplit.getPath()).thenReturn(new Path(path));
        when(context.getConfiguration()).thenReturn(new Configuration());
        reader.initialize(genericSplit, context);

    }

    @Test
    public void parseRecords() throws IOException, InterruptedException {
        int relationShipCount = 0;
        while (reader.nextKeyValue()) {
            relationShipCount++;
            System.out.println(reader.getCurrentValue());
        }
        assertEquals("Wrong Number of Relationships", 3, relationShipCount);
    }

    @After
    public void close() throws IOException {
        reader.close();
    }

}
