package org.neu.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * Created by rashmidwaraka on 10/4/17.
 */
public class ScoreCountWritable implements Writable {

    private long score;
    private long count;

    ScoreCountWritable(){
    }

    ScoreCountWritable(long score, long count){
        this.score = score;
        this.count = count;
    }

    public long getScore() {
        return score;
    }

    public long getCount() {
        return count;
    }

    public void setScore(long score) {
        this.score = score;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(score);
        out.writeLong(count);
    }

    public void readFields(DataInput in) throws IOException {
        score = in.readLong();
        count = in.readLong();
    }

}