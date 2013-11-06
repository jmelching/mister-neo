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

    private RelationshipRecordWritable record = null;
    private FSDataInputStream input;
    private long start;
    private long pos;
    private long end;

    @Override
    public void close() throws IOException {
        input.close();
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
        // boolean skipFirstLine = false;
        input = fs.open(split.getPath());

        // if (start != 0){
        // skipFirstLine = true;
        // --start;
        // filein.seek(start);
        // }
        // in = new LineReader(filein,conf);
        // if(skipFirstLine){
        // start += in.readLine(new
        // Text(),0,(int)Math.min((long)Integer.MAX_VALUE, end - start));
        // }
        this.pos = start;

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        try {
            int inUseByte = input.readUnsignedByte();
            boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();

            long firstNode = input.readInt();
            long firstNodeMod = (inUseByte & 0xEL) << 31;

            long secondNode = input.readInt();

            // [ xxx, ][ , ][ , ][ , ] second node high order bits, 0x70000000
            // [ ,xxx ][ , ][ , ][ , ] first prev rel high order bits, 0xE000000
            // [ , x][xx , ][ , ][ , ] first next rel high order bits, 0x1C00000
            // [ , ][ xx,x ][ , ][ , ] second prev rel high order bits, 0x380000
            // [ , ][ , xxx][ , ][ , ] second next rel high order bits, 0x70000
            // [ , ][ , ][xxxx,xxxx][xxxx,xxxx] type
            long typeInt = input.readInt();
            long secondNodeMod = (typeInt & 0x70000000L) << 4;
            int type = (int) (typeInt & 0xFFFF);

            // record = new RelationshipRecord(new Random().nextInt(),
            // longFromIntAndMod(firstNode, firstNodeMod),
            // longFromIntAndMod(secondNode, secondNodeMod), type);
            // record.setInUse(inUse);

            long firstPrevRel = input.readInt();
            // long firstPrevRelMod = (typeInt & 0xE000000L) << 7;
            // record.setFirstPrevRel(longFromIntAndMod(firstPrevRel,
            // firstPrevRelMod));

            long firstNextRel = input.readInt();
            // long firstNextRelMod = (typeInt & 0x1C00000L) << 10;
            // record.setFirstNextRel(longFromIntAndMod(firstNextRel,
            // firstNextRelMod));

            long secondPrevRel = input.readInt();
            // long secondPrevRelMod = (typeInt & 0x380000L) << 13;
            // record.setSecondPrevRel(longFromIntAndMod(secondPrevRel,
            // secondPrevRelMod));

            long secondNextRel = input.readInt();
            // long secondNextRelMod = (typeInt & 0x70000L) << 16;
            // record.setSecondNextRel(longFromIntAndMod(secondNextRel,
            // secondNextRelMod));

            long nextProp = input.readInt();
            // long nextPropMod = (inUseByte & 0xF0L) << 28;
            //
            // record.setNextProp(longFromIntAndMod(nextProp, nextPropMod));

            if (inUse) { // TODO: create reset
                record = new RelationshipRecordWritable(longFromIntAndMod(firstNode, firstNodeMod), longFromIntAndMod(
                        secondNode, secondNodeMod), type);
            }
        } catch (EOFException e) {
            return false;
        }
        return true;

    }

    protected long longFromIntAndMod(long base, long modifier) {
        return modifier == 0 && base == IdGeneratorImpl.INTEGER_MINUS_ONE ? -1 : base | modifier;
    }

}
