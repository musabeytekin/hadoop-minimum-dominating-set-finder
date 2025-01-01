package org.inonu.edu;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class GraphMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private final IntWritable node = new IntWritable();
    private final IntWritable neighbor = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] nodes = value.toString().split(",");
        if (nodes.length == 2) {
            int node1 = Integer.parseInt(nodes[0]);
            int node2 = Integer.parseInt(nodes[1]);

            node.set(node1);
            neighbor.set(node2);
            context.write(node, neighbor);

            node.set(node2);
            neighbor.set(node1);
            context.write(node, neighbor);
        }
    }
}