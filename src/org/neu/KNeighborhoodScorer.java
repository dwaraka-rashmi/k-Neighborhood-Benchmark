package org.neu;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by rashmidwaraka on 10/1/17.
 */
public class KNeighborhoodScorer {

    HashMap<String,ScoreCountPair> kNeighborScores = new HashMap<>();
    ArrayList<String> wordBuffer = new ArrayList<String>();
    LetterScorer letterScorer;
    Pattern p = Pattern.compile("[^A-Za-z ]");
    int k;

    KNeighborhoodScorer(int k, LetterScorer letterScorer) {
        this.k = k;
        this.letterScorer = letterScorer;
    }


    void processFiles(String inputFolderPath) throws IOException {

        final File folder = new File(inputFolderPath);
        for (final File fileEntry : folder.listFiles()) {
            String fileName = fileEntry.getName();
            if(!fileName.startsWith(".")) findKNeighborsInEachFile(inputFolderPath + fileName);
        }
    }


    void findKNeighborsInEachFile(String filePath) throws IOException {

        BufferedReader br = new BufferedReader(new FileReader(new File(filePath)));
        String line = null;
        while ((line = br.readLine()) != null)
            if(!line.trim().isEmpty()) {
//                StringBuilder sb = new StringBuilder(line);
//                p.matcher(sb).replaceAll("").toLowerCase();
                line = p.matcher(line).replaceAll("").toLowerCase();
//                line = line.replaceAll("[^A-Za-z ]", "").toLowerCase();
//                processLine(sb.toString().trim());
                processLine(line.trim());
            }
        br.close();

        /* Merge Last line's remaining words to the global neighbors */
        HashMap<Integer,ArrayList<String>> local = new HashMap<Integer,ArrayList<String>>();
        if(wordBuffer.size()!=0)
            for(int i =0;i<k-1;i++)
                local.put(i,new ArrayList<>(wordBuffer.subList(i+1,k)));

        mergeWordScoreToGlobal(wordBuffer,local,wordBuffer.size());
        System.out.println(filePath+" procesing completed!");
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
                ArrayList<String> kbackword = new ArrayList<>(currWordBuffer.subList(j-k, j));
                if(currKNeighbors.containsKey(j)) currKNeighbors.get(j).addAll(kbackword);
                else currKNeighbors.put(j, kbackword);
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


    public int getWordScore(String word){
        int total = 0;
        for(Character c : word.toCharArray()) total += letterScorer.letterScore.get(c);
        return total;
    }


    public int getWordListScore(ArrayList<String> neighborList) {
        int total = 0;
        for(String word : neighborList) total += getWordScore(word);
        return total;
    }


    public void mergeWordScoreToGlobal(ArrayList<String> localWordBuffer,
            HashMap<Integer,ArrayList<String>> local, Integer bufferLength) {

        for(Map.Entry<Integer,ArrayList<String>> e : local.entrySet()) {

            int index = e.getKey();
            String word = localWordBuffer.get(index);
            int score = getWordListScore(e.getValue());

            /*  The condition to check if the word's neighbors have been processed in previous line.
                If true, then append the words to the existing neighbor list of this occurrence */
            if(index<k && bufferLength!=0) {
                if(kNeighborScores.containsKey(word))
                    kNeighborScores.get(word).setScore(kNeighborScores.get(word).getScore() + score);
            }
            else {
                if(kNeighborScores.containsKey(word)) {
                    kNeighborScores.get(word).setScore(kNeighborScores.get(word).getScore() + score);
                    kNeighborScores.get(word).setCount(kNeighborScores.get(word).getCount() + 1);
                }
                else {
                    kNeighborScores.put(word, new ScoreCountPair());
                    kNeighborScores.get(word).setScore(kNeighborScores.get(word).getScore() + score);
                    kNeighborScores.get(word).setCount(kNeighborScores.get(word).getCount() + 1);
                }
            }
        }
    }


    void printScores(String fileName) throws IOException {

        BufferedWriter bw = new BufferedWriter(new FileWriter(fileName,true));
        TreeMap<String,ScoreCountPair> kNeighborScore = new TreeMap(this.kNeighborScores);

        for(Map.Entry<String,ScoreCountPair> e : kNeighborScore.entrySet()) {
            long total = e.getValue().getScore(), count = e.getValue().getCount();
            Double meanScore = total/(double)count;
            StringBuilder sb = new StringBuilder();
            sb.append(e.getKey()+','+meanScore+'\n');
            bw.write(sb.toString());
            bw.flush();
        }
        bw.close();
    }


    void combineKNeighbors(HashMap<String,ScoreCountPair> kNeighbors) {

        for(Map.Entry<String,ScoreCountPair> e : kNeighbors.entrySet()){
            String s = e.getKey();
            long score = e.getValue().getScore();
            long count = e.getValue().getCount();

            if(kNeighborScores.containsKey(s)) {
                kNeighborScores.get(s).setScore(kNeighborScores.get(s).getScore() + score);
                kNeighborScores.get(s).setCount(kNeighborScores.get(s).getCount() + count);
            }
            else kNeighborScores.put(s,new ScoreCountPair(score,count));
        }
    }

}
