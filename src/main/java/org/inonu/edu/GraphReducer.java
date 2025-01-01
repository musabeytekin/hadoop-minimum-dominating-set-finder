package org.inonu.edu;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class GraphReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
    private Map<Integer, Set<Integer>> adjacencyList = new HashMap<>();
    private Set<Integer> dominatingSet = new HashSet<>();
    private Set<Integer> coveredNodes = new HashSet<>();

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Set<Integer> neighbors = new HashSet<>();
        for (IntWritable val : values) {
            neighbors.add(val.get());
        }
        adjacencyList.put(key.get(), neighbors);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Process nodes to find dominating set
        while (coveredNodes.size() < adjacencyList.size()) {
            int bestNode = -1;
            int maxUncoveredNeighbors = -1;
            Set<Integer> bestCoverage = null;

            for (Map.Entry<Integer, Set<Integer>> entry : adjacencyList.entrySet()) {
                int node = entry.getKey();
                if (dominatingSet.contains(node)) continue;

                Set<Integer> uncoveredNeighbors = new HashSet<>(entry.getValue());
                uncoveredNeighbors.removeAll(coveredNodes);
                if (!coveredNodes.contains(node)) {
                    uncoveredNeighbors.add(node);
                }

                if (uncoveredNeighbors.size() > maxUncoveredNeighbors) {
                    maxUncoveredNeighbors = uncoveredNeighbors.size();
                    bestNode = node;
                    bestCoverage = new HashSet<>(uncoveredNeighbors);
                }
            }

            if (bestNode != -1) {
                dominatingSet.add(bestNode);
                coveredNodes.add(bestNode);
                coveredNodes.addAll(adjacencyList.get(bestNode));
            }
        }

        // Output nodes data
        context.write(new IntWritable(-1), new Text("NODES_START"));
        for (int node : adjacencyList.keySet()) {
            String nodeType = dominatingSet.contains(node) ? "dominating" : "normal";
            String nodeData = String.format("{\"id\": %d, \"type\": \"%s\"}", node, nodeType);
            context.write(new IntWritable(-1), new Text(nodeData));
        }
        context.write(new IntWritable(-1), new Text("NODES_END"));

        // Output edges data
        context.write(new IntWritable(-1), new Text("EDGES_START"));
        for (Map.Entry<Integer, Set<Integer>> entry : adjacencyList.entrySet()) {
            int source = entry.getKey();
            for (int target : entry.getValue()) {
                if (source < target) { // Only output each edge once
                    String edgeData = String.format("{\"source\": %d, \"target\": %d}", source, target);
                    context.write(new IntWritable(-1), new Text(edgeData));
                }
            }
        }
        context.write(new IntWritable(-1), new Text("EDGES_END"));
    }
}