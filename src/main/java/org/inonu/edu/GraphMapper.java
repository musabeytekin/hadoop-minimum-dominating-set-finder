package org.inonu.edu;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GraphMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private final IntWritable node = new IntWritable();
    private final Text neighborWithWeight = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] nodes = value.toString().split(",");
        if (nodes.length == 3) {
            int node1 = Integer.parseInt(nodes[0]);
            int node2 = Integer.parseInt(nodes[1]);
            double weight = Double.parseDouble(nodes[2]);

            if (weight >= 0.7) { // Sadece güçlü bağlantıları dahil et
                node.set(node1);
                neighborWithWeight.set(nodes[1] + "," + nodes[2]);
                context.write(node, neighborWithWeight);

                node.set(node2);
                neighborWithWeight.set(nodes[0] + "," + nodes[2]);
                context.write(node, neighborWithWeight);
            }
        }
    }
}