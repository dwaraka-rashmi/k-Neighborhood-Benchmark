package org.neu.mapreduceMedian;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rashmidwaraka on 10/5/17.
 */
public class KNeighborhoodScorerCombiner
        extends Reducer<Text,ScoreCountWritable,Text,ScoreCountWritable> {

    public void reduce(Text key, Iterable<ScoreCountWritable> values, Context context)
            throws IOException, InterruptedException {

        HashMap<Integer,Integer> freqScore = new HashMap<>();

        for (ScoreCountWritable val : values) {
            int score = val.getScore();
            int count = val.getCount();
            if(freqScore.containsKey(score))
                freqScore.put(score,freqScore.get(score)+count);
            else
                freqScore.put(score,count);
        }

        for(Map.Entry<Integer,Integer> e : freqScore.entrySet()) {
            context.write(key, new ScoreCountWritable(e.getKey(), e.getValue()));
        }
    }
}
