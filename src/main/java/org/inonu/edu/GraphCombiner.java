package org.inonu.edu;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class GraphCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Set<Integer> combined = new HashSet<>();
        for (IntWritable val : values) {
            combined.add(val.get());
        }
        
        IntWritable outValue = new IntWritable();
        for (Integer value : combined) {
            outValue.set(value);
            context.write(key, outValue);
        }
    }
}