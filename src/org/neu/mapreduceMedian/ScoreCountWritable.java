package org.neu.mapreduceMedian;

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

    private int score;
    private int count;

    ScoreCountWritable(){
    }

    ScoreCountWritable(int score, int count){
        this.score = score;
        this.count = count;
    }

    public int getScore() {
        return score;
    }

    public int getCount() {
        return count;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(score);
        out.writeInt(count);
    }

    public void readFields(DataInput in) throws IOException {
        score = in.readInt();
        count = in.readInt();
    }

}