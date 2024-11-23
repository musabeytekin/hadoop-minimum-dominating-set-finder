package org.inonu.edu;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class GraphReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Create a set of neighbors for the current node
        Set<String> neighbors = new HashSet<>();
        for (Text value : values) {
            neighbors.add(value.toString());
        }

        // Write the node and its neighbors to the output
        StringBuilder sb = new StringBuilder();
        for (String neighbor : neighbors) {
            sb.append(neighbor).append(",");
        }

        // Remove trailing comma
        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }

        context.write(key, new Text(sb.toString()));
    }
}