package org.neu.mapreduceMedian;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import sun.awt.image.ImageWatched;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by rashmidwaraka on 10/4/17.
 */
public class KNeighborReducer
        extends Reducer<Text,ScoreCountWritable,Text,FloatWritable> {

    public void reduce(Text key, Iterable<ScoreCountWritable> values, Context context)
            throws IOException, InterruptedException {

        HashMap<Integer, Integer> freqList = new HashMap<>();
        for (ScoreCountWritable val : values) {
            int count = val.getCount();
            int score = val.getScore();
            if (freqList.containsKey(score))
                freqList.put(score, freqList.get(score) + count);
            else
                freqList.put(score, count);
        }

        TreeMap<Integer, Integer> sortedFreqList = new TreeMap<>(freqList);
        LinkedList<Integer> sortedKeys = new LinkedList<>();

        for (Map.Entry<Integer, Integer> e : sortedFreqList.entrySet()) {
            int val = e.getKey();
            for (int i = 0; i < e.getValue(); i++)
                sortedKeys.add(val);
        }
        context.write(key,new FloatWritable(calcMedian(sortedKeys)));
    }


    public float calcMedian(LinkedList<Integer> sortedList){
        int len = sortedList.size();
        if(len==1) return sortedList.get(0);
        if(len%2 == 0) return (float)(sortedList.get(len/2)+sortedList.get((len/2)-1))/2;
        else return (float)(sortedList.get((int) Math.floor((len+1)/2)));
    }

}
