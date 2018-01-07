package org.neu.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by rashmidwaraka on 10/4/17.
 */
public class LetterCountReducer extends
        Reducer<Text,LongWritable,Text,LongWritable> {

    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable val : values) sum += val.get();
        context.write(key, new LongWritable(sum));
    }

}

