package org.neu.mapreduceMedian;

/**
 * Created by rashmidwaraka on 10/2/17.
 */

/**
 * ScoreCountPair class stores the total score of the kNeighborhood sets
 * and the count of Kneighborhood sets
 */
public class ScoreCountPair {
    int totalScore;
    int totalSetCount;

    public ScoreCountPair() {
        this.totalScore = 0;
        this.totalSetCount = 0;
    }

    public ScoreCountPair(int score, int count) {
        this.totalScore = score;
        this.totalSetCount = count;
    }

    public int getScore() {
        return totalScore;
    }

    public void setScore(int score) {
        this.totalScore = score;
    }

    public int getCount() {
        return totalSetCount;
    }

    public void setCount(int count) {
        this.totalSetCount = count;
    }
}
