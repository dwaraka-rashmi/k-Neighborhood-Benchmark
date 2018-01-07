package org.neu;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.util.concurrent.Callable;

/**
 * Created by rashmidwaraka on 10/1/17.
 */
public class ParallelLetterScorer implements Callable<LetterScorer>{

    String fileName;

    ParallelLetterScorer(String fileName){
        this.fileName = fileName;
    }

    @Override
    public LetterScorer call() throws Exception {
        LetterScorer partialLetterCount = new LetterScorer();
        partialLetterCount.countLettersInEachfile(fileName);
        return partialLetterCount;
    }

}
