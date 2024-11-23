package org.inonu.edu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DominatingSetJob {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DominatingSetJob <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("input.path", args[0]);
        Job job = Job.getInstance(conf, "Dominating Set");

        job.setJarByClass(DominatingSetJob.class);
        job.setMapperClass(GraphMapper.class);
        job.setCombinerClass(GraphCombiner.class);
        job.setReducerClass(GraphReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}