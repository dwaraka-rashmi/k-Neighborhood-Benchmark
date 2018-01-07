package org.neu.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * Created by rashmidwaraka on 10/4/17.
 */
public class CustomFileInputSplit extends TextInputFormat {
    @Override
    protected  boolean isSplitable(JobContext context,Path file) {
        return false;
    }
}