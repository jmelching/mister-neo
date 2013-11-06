package org.jmelching.misterneo.formats;

import static org.junit.Assert.assertTrue;

import org.jmelching.misterneo.formats.RelationshipInputFormat;
import org.jmelching.misterneo.formats.RelationshipRecordReader;
import org.junit.Test;

public class RelationshipInputFormatTest {
    
    @Test
    public void verifyRecordReader(){
       assertTrue( new RelationshipInputFormat().createRecordReader(null, null).getClass().equals(RelationshipRecordReader.class));
    }
    
    @Test
    public void verifySupportsSplitable() {
        assertTrue(new RelationshipInputFormat().isSplitable(null, null));
    }

}
