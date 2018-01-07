package org.neu.mapreduce;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by rashmidwaraka on 10/4/17.
 */
public class KNeighborReducer
        extends Reducer<Text,ScoreCountWritable,Text,FloatWritable> {

    public void reduce(Text key, Iterable<ScoreCountWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0, count = 0;
        for (ScoreCountWritable val : values) {
            sum += val.getScore();
            count += val.getCount();
        }
        context.write(key, new FloatWritable(sum/(float)count));
    }
}
