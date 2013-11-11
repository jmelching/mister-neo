package org.jmelching.misterneo.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class RelationshipRecordWritable implements Writable {
    private long firstNode;
    private long secondNode;
    private int type;

    public RelationshipRecordWritable() {
    }

    public void setValue(long firstNode, long secondNode, int type) {
        this.firstNode = firstNode;
        this.secondNode = secondNode;
        this.type = type;
    }

    public void readFields(DataInput in) throws IOException {
        this.firstNode = in.readLong();
        this.secondNode = in.readLong();
        this.type = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(firstNode);
        out.writeLong(secondNode);
        out.writeInt(this.type);

    }

    public long getFirstNode() {
        return this.firstNode;
    }

    public long getSecondNode() {
       return this.secondNode;
    }
    
  

}
