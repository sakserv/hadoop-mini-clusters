package com.github.sakserv.minicluster.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text word = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(Object key, Text value,
                    Context contex) throws IOException, InterruptedException {
        // Break line into words for processing
        StringTokenizer wordList = new StringTokenizer(value.toString());
        while (wordList.hasMoreTokens()) {
            word.set(wordList.nextToken());
            contex.write(word, one);
        }
    }
}