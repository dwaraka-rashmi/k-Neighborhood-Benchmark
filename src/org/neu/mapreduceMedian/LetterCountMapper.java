package org.neu.mapreduceMedian;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rashmidwaraka on 10/4/17.
 */
public class LetterCountMapper
        extends Mapper<Object, Text, Text, LongWritable> {

    private HashMap<Character,Long> letterCount = new HashMap<>();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        if(!line.trim().isEmpty()) {
            line = line.replaceAll("[^A-Za-z ]", "").toLowerCase();
            for(Character c : line.toCharArray()) {
                if(c!=' ') {
                    if(letterCount.containsKey(c))
                        letterCount.put(c,letterCount.get(c)+1);
                    else letterCount.put(c,1l);
                }
            }
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        int totalCount = 0;
        for(Map.Entry<Character,Long> e : letterCount.entrySet()) {
            long val = e.getValue();
            context.write(new Text(String.valueOf(e.getKey())), new LongWritable(val));
            totalCount +=val;
        }
        context.write(new Text("total"), new LongWritable(totalCount));
    }
}
