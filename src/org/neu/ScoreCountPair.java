package org.neu;

/**
 * Created by rashmidwaraka on 10/2/17.
 */

/**
 * ScoreCountPair class stores the total score of the kNeighborhood sets
 * and the count of Kneighborhood sets
 */
public class ScoreCountPair {
    long totalScore;
    long totalSetCount;

    public ScoreCountPair() {
        this.totalScore = 0;
        this.totalSetCount = 0;
    }

    public ScoreCountPair(long score, long count) {
        this.totalScore = score;
        this.totalSetCount = count;
    }

    public long getScore() {
        return totalScore;
    }

    public void setScore(long score) {
        this.totalScore = score;
    }

    public long getCount() {
        return totalSetCount;
    }

    public void setCount(long count) {
        this.totalSetCount = count;
    }
}
