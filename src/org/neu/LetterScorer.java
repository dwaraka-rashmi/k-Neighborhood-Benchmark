package org.neu;

import java.util.HashMap;
import java.util.Map;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by rashmidwaraka on 10/1/17.
 */
public class LetterScorer {

    HashMap<Character,Integer> letterCount = new HashMap<Character,Integer>();
    HashMap<Character,Integer> letterScore = new HashMap<Character,Integer>();
    long corpusSize;

    LetterScorer() {
        corpusSize = 0;
    }

    void printLetterCount() {
        for(Map.Entry<Character,Integer> e : letterCount.entrySet())
            System.out.println(e.getKey()+" => "+e.getValue());
    }

    void printLetterScores() {
        for(Map.Entry<Character,Integer> e : letterScore.entrySet())
            System.out.println(e.getKey()+" => "+e.getValue());
    }

    int getScore(Double val) {
        if(val >= 10.0) return 0;
        if(val >= 8.0) return 1;
        if(val >= 6.0) return 2;
        if(val >= 4.0) return 4;
        if(val >= 2.0) return 8;
        if(val >= 1.0) return 16;
        else return 32;
    }

    void scoreLetters() throws IOException {
        for(Map.Entry<Character,Integer> e : letterCount.entrySet()) {
            letterScore.put(e.getKey(), getScore((e.getValue() / (double) corpusSize) * 100));
        }
    }

    void countLettersInEachfile(String filePath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(new File(filePath)));
        String line = null;

        while ((line = br.readLine()) != null) {
            if(!line.trim().isEmpty()) {
                line = line.replaceAll("[^A-Za-z ]", "").toLowerCase();
                for(Character c : line.toCharArray()) {
                    if(c!=' ') {
                        corpusSize += 1;
                        if(letterCount.containsKey(c))
                            letterCount.put(c,letterCount.get(c)+1);
                        else letterCount.put(c,1);
                    }
                }
            }
        }
        br.close();
        System.out.println(filePath+" procesing completed!");
    }

    void processFiles(String folderPath) throws IOException{

        final File folder = new File(folderPath);

        for (final File fileEntry : folder.listFiles()) {
            String fileName = fileEntry.getName();
            String filePath = folderPath + fileName;
            if(!fileName.startsWith("."))
                countLettersInEachfile(filePath);
        }
        scoreLetters();
    }

    void addLetterCount(HashMap<Character,Integer> localLetterCount) {

        for(Map.Entry<Character, Integer> e : localLetterCount.entrySet()) {
            Character c = e.getKey();
            if(letterCount.containsKey(c))
                letterCount.put(c, (letterCount.get(c) + localLetterCount.get(c)));
            else
                letterCount.put(c,localLetterCount.get(c));
        }

        for(Integer count : localLetterCount.values())
            corpusSize += count;

    }


}
