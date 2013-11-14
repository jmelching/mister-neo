package org.jmelching.misterneo.formats;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.jmelching.misterneo.writables.RelationshipRecordWritable;
import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;
import org.neo4j.kernel.impl.nioneo.store.Record;

public class RelationshipRecordReader extends RecordReader<NullWritable, RelationshipRecordWritable> {

    private static final int RECORD_LENGTH = 33;
    private RelationshipRecordWritable record = new RelationshipRecordWritable();
    private FSDataInputStream input;
    private long start;
    private long pos;
    private long end;

    @Override
    public void close() throws IOException {
        input.close();
        System.out.println("POS: " + pos);
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public RelationshipRecordWritable getCurrentValue() throws IOException, InterruptedException {
        return record;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException,
            InterruptedException {
        FileSplit split = (FileSplit) genericSplit;
        final Path file = split.getPath();
        Configuration conf = context.getConfiguration();
        FileSystem fs = file.getFileSystem(conf);
        start = split.getStart();
        end = start + split.getLength();
        input = fs.open(split.getPath());
        if (start != 0) {
            input.seek(start + (RECORD_LENGTH - (start % RECORD_LENGTH)));
        }
        // TODO handle the trailing header info
        System.out.println("Start: " + start + " End: " + end);
        if (start != 0) {
            System.out.println("Seek: " + (RECORD_LENGTH - (start % RECORD_LENGTH)));
        }
        this.pos = start;

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean inUse = false;
        while (pos < end && !inUse) {
            try {
                int inUseByte = input.readUnsignedByte();
                inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();

                long firstNode = input.readInt();
                long firstNodeMod = (inUseByte & 0xEL) << 31;

                long secondNode = input.readInt();

                long typeInt = input.readInt();
                long secondNodeMod = (typeInt & 0x70000000L) << 4;
                int type = (int) (typeInt & 0xFFFF);

                long firstPrevRel = input.readInt();

                long firstNextRel = input.readInt();

                long secondPrevRel = input.readInt();

                long secondNextRel = input.readInt();

                long nextProp = input.readInt();
                pos = pos + RECORD_LENGTH;
                if (inUse) {
                    record.setValue(longFromIntAndMod(firstNode, firstNodeMod),
                            longFromIntAndMod(secondNode, secondNodeMod), type);
                    return true;
                }

            } catch (EOFException e) {
                return false;
            }
        }
        return false;

    }

    protected long longFromIntAndMod(long base, long modifier) {
        return modifier == 0 && base == IdGeneratorImpl.INTEGER_MINUS_ONE ? -1 : base | modifier;
    }

}
