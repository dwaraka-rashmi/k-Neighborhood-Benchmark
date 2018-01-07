package org.neu.mapreduceMedian;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rashmidwaraka on 10/4/17.
 */
public class InitNeighborhoodMedianScorerMapReduce {

    public static long getScore(Double val) {
        if (val >= 10.0) return 0;
        if (val >= 8.0) return 1;
        if (val >= 6.0) return 2;
        if (val >= 4.0) return 4;
        if (val >= 2.0) return 8;
        if (val >= 1.0) return 16;
        else return 32;
    }


    public static void scoreLettersWriteToHDFS(Configuration conf) throws IOException{

        FileSystem fs = FileSystem.get(conf);
        HashMap<String,Long> letterStats = new HashMap<>();

        Path pt=new Path("letter-count/part-r-00000");
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        while ((line = br.readLine()) != null) {
            String[] split = line.split("\t");
            letterStats.put(split[0],Long.parseLong(split[1]));
        }
        br.close();

        long totalLetterCount = letterStats.get("total");
        for(Map.Entry<String,Long> e : letterStats.entrySet()) {
            String c = e.getKey();
            if (!c.matches("total"))
                letterStats.put(c, getScore(((e.getValue() / (double)totalLetterCount) * 100)));
        }

        Path dst= new Path("letter-score");
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(dst)));
        for(Map.Entry<String,Long> e : letterStats.entrySet())
            if(!e.getKey().matches("total"))
                bw.write(e.getKey() + "\t" + e.getValue() + "\n");
        bw.close();
    }


    public static Boolean letterScorerMapReduceSetup(String input, String output)
            throws Exception{

        // BufferedWriter bw = new BufferedWriter(new FileWriter("output/benchmark.csv",true));
        // long startTime = System.nanoTime();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Letter Count");

        job.setJarByClass(InitNeighborhoodMedianScorerMapReduce.class);
        job.setMapperClass(LetterCountMapper.class);
        job.setReducerClass(LetterCountReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        Boolean jobStatus = job.waitForCompletion(true);
        if(jobStatus && job.isSuccessful()) scoreLettersWriteToHDFS(conf);
        return jobStatus;
    }


    public static Boolean dataCleanserIntoOverlappingBlocks(String k,String input)
            throws IOException, InterruptedException, Exception{

        Boolean jobStatus = false;
        Configuration conf = new Configuration();
        conf.set("K",k);

        Job job = Job.getInstance(conf, "Data Cleanser");
        job.setJarByClass(InitNeighborhoodMedianScorerMapReduce.class);

        job.setMapperClass(DataCleanerMapper.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(CustomFileInputSplit.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path("tmp-output"));

        if(job.waitForCompletion(true) && job.isSuccessful()) {
            return true;
        }
        return jobStatus;
    }


    public static Boolean KNeighborhoodScorerMapReduceSetup(String k,String input, String output)
            throws Exception {

        Boolean jobStatus = false;
        if(dataCleanserIntoOverlappingBlocks(k,input)) {

            Configuration conf = new Configuration();
            conf.set("K", k);
            conf.set("mapreduce.output.textoutputformat.separator", ",");

            Job job = Job.getInstance(conf, "KNeighborhood Count");
            job.setJarByClass(InitNeighborhoodMedianScorerMapReduce.class);
            job.setMapperClass(KNeighborhoodMedianMapper.class);
            job.setCombinerClass(KNeighborhoodScorerCombiner.class);
            job.setReducerClass(KNeighborReducer.class);
            job.setInputFormatClass(CustomFileInputSplit.class);
            job.setNumReduceTasks(1);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(ScoreCountWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FloatWritable.class);

            FileInputFormat.addInputPath(job, new Path("tmp-output"));
            FileOutputFormat.setOutputPath(job, new Path(output));

            jobStatus = job.waitForCompletion(true);
            if (jobStatus && job.isSuccessful()) {

                 FileSystem fs = FileSystem.get(conf);

                 if(fs.exists(new Path(output))) {
                     fs.delete(new Path(output),true);
                     System.out.println("output deleted!");
                 }
                 if(fs.exists(new Path("letter-score"))) {
                     fs.delete(new Path("letter-score"),true);
                     System.out.println("Letter score deleted!");
                 }
                 if(fs.exists(new Path("letter-count"))) {
                     fs.delete(new Path("letter-count"),true);
                     System.out.println("Letter count deleted!");
                 }
                if(fs.exists(new Path("tmp-output"))) {
                    fs.delete(new Path("tmp-output"),true);
                    System.out.println("tmp-output deleted!");
                }
            }
        }
        return jobStatus;
    }


    public static void main(String[] args) throws Exception {

        int iterations = Integer.parseInt(args[0]);
        for(int i = 1; i<=iterations ;i++) {
             long startTime = System.nanoTime();
            if (letterScorerMapReduceSetup(args[2], "letter-count")) {
                 System.out.println("Letter Scorer module completed! ");
                if (KNeighborhoodScorerMapReduceSetup(args[1], args[2], args[3]))
                     System.out.println("KNeighborhood Scorer module completed!");
            }
             long endTime = System.nanoTime();
            System.out.println("total time"+(endTime-startTime)/1000000);
        }
    }

}





