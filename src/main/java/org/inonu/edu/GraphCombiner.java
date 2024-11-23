package org.inonu.edu;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class GraphCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> combined = new HashSet<>();
        for (Text val : values) {
            combined.add(val.toString());
        }
        context.write(key, new Text(String.join(",", combined)));
    }
}