package org.neu.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by rashmidwaraka on 10/5/17.
 */
public class DataCleanerMapper extends Mapper<Object, Text, Text, Text> {

    int k;
    Pattern p = Pattern.compile("[^A-Za-z ]");
    LinkedList<String> wordList = new LinkedList<>();

    public void setup(Context context) throws IOException, InterruptedException {
        k = Integer.parseInt(context.getConfiguration().get("K"));
    }


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(!line.trim().isEmpty()) {
            line = p.matcher(line).replaceAll("").toLowerCase();
            processLine(line.trim(),context);
        }
    }


    public void processLine(String line,Context context) throws IOException, InterruptedException {
        ArrayList<String> currWordList = new ArrayList<>(Arrays.asList(line.split("\\s+")));
        if(wordList.size() > 100) {
            printToContext(context);
            wordList.clear(); //clear the current word block
        }
        else wordList.addAll(currWordList);
    }


    public void printToContext(Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        for (String value : wordList) builder.append(value+" ");
        builder.append("\n");
        context.write(new Text(""), new Text(builder.toString()));
    }


    public void cleanup(Context context) throws IOException, InterruptedException {
        printToContext(context);
    }
}

