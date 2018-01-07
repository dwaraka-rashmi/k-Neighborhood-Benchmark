package org.neu.mapreduce;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.neu.ScoreCountPair;

import java.io.*;
import java.util.*;

/**
 * Created by rashmidwaraka on 10/4/17.
 */
public class KNeighborMapper extends Mapper<Object, Text, Text, ScoreCountWritable>{

    HashMap<Character,Long> letterStats = new HashMap<>();
    int k;

    HashMap<String,ScoreCountPair> kNeighborScores = new HashMap<>();
    ArrayList<String> wordBuffer = new ArrayList<String>();

    public void setup(Context context) throws IOException, InterruptedException {

        FileSystem fs = FileSystem.get(context.getConfiguration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path("letter-score"))));
        String line;
        while ((line = br.readLine()) != null) {
            String[] split = line.split("\t");
            letterStats.put(split[0].charAt(0),Long.parseLong(split[1]));
        }
        br.close();

        k = Integer.parseInt(context.getConfiguration().get("K"));
    }


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        processLine(value.toString());
    }


    public long getWordScore(String word){
        long total = 0;
        for(Character c : word.toCharArray())
            total += letterStats.get(c);
        return total;
    }


    public long getWordListScore(List<String> neighborList) {
        long total = 0;
        for(String word : neighborList) total += getWordScore(word);
        return total;
    }


    public void processLine(String line) {

        ArrayList<String> currWordList = new ArrayList<>(Arrays.asList(line.split("\\s+")));
        ArrayList<String> currWordBuffer = wordBuffer;
        HashMap<Integer,ArrayList<String>> currKNeighbors = new HashMap<>();

        int initialBufferLength = currWordBuffer.size();
        currWordBuffer.addAll(currWordList);
        int currBufferLength = currWordBuffer.size();

        /* Forward K Neighbors and Backword K Neighbors */
        for(int i = 0, j = k; (i < currBufferLength-k) || (j < currBufferLength); i++, j++) {

            if(i < (currBufferLength-k)) {
                ArrayList<String> kforward = new ArrayList<>(currWordBuffer.subList(i + 1, i + k + 1));
                if (currKNeighbors.containsKey(i)) currKNeighbors.get(i).addAll(kforward);
                else currKNeighbors.put(i, kforward);
            }

            if(j < currBufferLength) {
                if(currKNeighbors.containsKey(j))
                    currKNeighbors.get(j).addAll(new ArrayList<>(currWordBuffer.subList(j-k, j)));
                else currKNeighbors.put(j, new ArrayList<>(currWordBuffer.subList(j-k, j)));
            }
        }

        /* When parsing first line of the file, the buffer will be empty. The below code
            snippet handles the k backword words for first k words in the line */
        if(initialBufferLength==0) {
            for (int i = 1; i < k && i < currBufferLength; i++) {
                if(currKNeighbors.containsKey(i))
                    currKNeighbors.get(i).addAll(currWordBuffer.subList(0, i));
                else currKNeighbors.put(i, new ArrayList<>(currWordBuffer.subList(0, i)));
            }
        }

        mergeWordScoreToGlobal(currWordBuffer,currKNeighbors,initialBufferLength);
        if(currBufferLength<k) wordBuffer = new ArrayList<>(currWordBuffer);
        else wordBuffer = new ArrayList<>(currWordBuffer.subList(currBufferLength - k, currBufferLength));
    }



    public void mergeWordScoreToGlobal(ArrayList<String> localWordBuffer,
                                       HashMap<Integer,ArrayList<String>> local, Integer bufferLength) {

        for(Map.Entry<Integer,ArrayList<String>> e : local.entrySet()) {
            int index = e.getKey();
            String word = localWordBuffer.get(index);
            long score = getWordListScore(e.getValue());

            if(kNeighborScores.containsKey(word)) {
                /*  The condition to check if the word's neighbors have been processed in previous line.
                If true, then append the words to the existing neighbor list of this occurrence */
                if(index<k && bufferLength!=0)
                    kNeighborScores.get(word).setScore(kNeighborScores.get(word).getScore() + score);
                else {
                    kNeighborScores.get(word).setScore(kNeighborScores.get(word).getScore() + score);
                    kNeighborScores.get(word).setCount(kNeighborScores.get(word).getCount() + 1);
                }
            }
            else kNeighborScores.put(word, new ScoreCountPair(score, 1));
        }
    }


    public void cleanup(Context context) throws IOException, InterruptedException {

        /* Merge Last line's remaining words to the global neighbors */
        HashMap<Integer,ArrayList<String>> local = new HashMap<Integer,ArrayList<String>>();
        if(wordBuffer.size()!=0) {
            for (int i = 0; i < k - 1; i++)
                local.put(i, new ArrayList<>(wordBuffer.subList(i + 1, k)));
            mergeWordScoreToGlobal(wordBuffer,local,wordBuffer.size());
        }
        printScores(context);
    }


    void printScores(Context context) throws IOException,InterruptedException {

        TreeMap<String,ScoreCountPair> kNeighborScore = new TreeMap(this.kNeighborScores);

        for(Map.Entry<String,ScoreCountPair> e : kNeighborScore.entrySet())
            context.write(new Text(e.getKey()),
                    new ScoreCountWritable(e.getValue().getScore(), e.getValue().getCount()));
    }

}

