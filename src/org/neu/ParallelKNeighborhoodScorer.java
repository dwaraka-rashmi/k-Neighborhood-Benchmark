package org.neu;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * Created by rashmidwaraka on 10/1/17.
 */
public class ParallelKNeighborhoodScorer implements Callable<KNeighborhoodScorer> {

    String fileName;
    int k;
    LetterScorer letterScore;

    ParallelKNeighborhoodScorer(String fileName, int k, LetterScorer letterScore){
        this.fileName = fileName;
        this.k = k;
        this.letterScore = letterScore;
    }

    public KNeighborhoodScorer call() throws Exception {
        KNeighborhoodScorer partialKNeighborScore = new KNeighborhoodScorer(k,letterScore);
        partialKNeighborScore.findKNeighborsInEachFile(fileName);
        return partialKNeighborScore;
    }

}

