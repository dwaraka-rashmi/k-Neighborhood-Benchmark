package org.neu;

import java.lang.IllegalArgumentException;
import java.io.IOException;
import java.io.File;
import java.util.concurrent.*;

public class InitNeighborhoodScorer {


    public static void serialScorerSetup(
            int k, String inputPath, String outputPath,int iterations) throws IOException {

        for(int i = 0; i < iterations; i++) {
            long startTime = System.nanoTime();
            LetterScorer letterScorer = new LetterScorer();
            letterScorer.processFiles(inputPath);
            letterScorer.printLetterCount();
            letterScorer.printLetterScores();
            System.out.println("Letter Scorer complete!");

            KNeighborhoodScorer kNeighborhoodScorer = new KNeighborhoodScorer(k, letterScorer);
            kNeighborhoodScorer.processFiles(inputPath);
            kNeighborhoodScorer.printScores(outputPath+"serial_"+i+".csv");

            System.out.println("KNeighborhood Scorer complete!");
            long endTime = System.nanoTime();
            System.out.println("Total time " + (endTime - startTime) / 1000000);
        }
    }


    public static LetterScorer scoreLettersParallel(int threadCount,String input)
            throws IOException, InterruptedException {

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CompletionService<LetterScorer> taskCompletionService = new ExecutorCompletionService<>(executor);
        LetterScorer totalLetterStat = new LetterScorer();

        try{
            final File folder = new File(input);
            int callableCount = 0;
            for (final File fileEntry : folder.listFiles()) {
                String fileName = fileEntry.getName();
                if (!fileName.startsWith(".")) {
                    taskCompletionService.submit(new ParallelLetterScorer(input + fileName));
                    callableCount++;
                }
            }

            for (int i = 0; i < callableCount; i++) {
                Future<LetterScorer> result = taskCompletionService.take();
                totalLetterStat.addLetterCount(result.get().letterCount);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        executor.shutdown();
        return totalLetterStat;
    }


    public static KNeighborhoodScorer scoreKNeighborhoodParallel(
            int k,LetterScorer letterScore,int threadCount, String input)
            throws IOException, InterruptedException {

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CompletionService<KNeighborhoodScorer> taskCompletionService = new ExecutorCompletionService<>(
                executor);
        KNeighborhoodScorer totalKNeighborScore = new KNeighborhoodScorer(k, letterScore);

        try{

            final File folder = new File(input);
            int callableCount = 0;
            for (final File fileEntry : folder.listFiles()) {
                String fileName = fileEntry.getName();
                if (!fileName.startsWith(".")) {
                    taskCompletionService.submit(new ParallelKNeighborhoodScorer(input + fileName, k, letterScore));
                    callableCount++;
                }
            }

            for(int i = 0; i< callableCount; i++) {
                Future<KNeighborhoodScorer> result = taskCompletionService.take();
                totalKNeighborScore.combineKNeighbors(result.get().kNeighborScores);
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        executor.shutdown();
        return totalKNeighborScore;
    }


    public static void parallelScorerSetup(
            int k, int threadCount, String inputPath, String outputPath, int iterations)
            throws IOException, InterruptedException {

        for(int tc = 2;tc <= 16; tc++) {
            for(int i = 0; i < iterations; i++) {

                long startTime = System.nanoTime();
                LetterScorer letterScorer = scoreLettersParallel(tc,inputPath);
                letterScorer.scoreLetters();

                System.out.println("Letter Score Completed");

                KNeighborhoodScorer kNeighborhoodScorer = scoreKNeighborhoodParallel(k,letterScorer,tc,inputPath);
                kNeighborhoodScorer.printScores(outputPath+"/parallel_"+i+".csv");

                System.out.println("K NeighborScore Score Completed");
                long endTime = System.nanoTime();
                System.out.println("Total time for thread count "+tc+" is " + (endTime - startTime) / 1000000);
            }
        }
    }


    public static void main(String[] args)
            throws IllegalArgumentException, IOException, InterruptedException {

        int k = 2,iterations = 1,threadCount = 2;
        String inputPath,outputPath;

        try{
            if(args.length>3){
                if(args[0].matches("serial")) {
                    k = Integer.parseInt(args[1]);
                    inputPath = args[2];
                    outputPath = args[3];
                    iterations = Integer.parseInt(args[4]);
                    serialScorerSetup(k,inputPath,outputPath,iterations);
                }
                else if(args[0].matches("parallel")) {
                    k = Integer.parseInt(args[1]);
                    threadCount = Integer.parseInt(args[2]);
                    inputPath = args[3];
                    outputPath = args[4];
                    iterations = Integer.parseInt(args[5]);
                    parallelScorerSetup(k,threadCount,inputPath,outputPath,iterations);
                }
                else System.out.println("Kindly enter the required parameters - mode, K, iterations");
            }
            else System.out.println("Kindly enter the required parameters - mode, K, iterations");
        }
        catch(Throwable e){
            throw e;
        }

    }

}
