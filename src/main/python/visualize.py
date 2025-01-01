import networkx as nx
import matplotlib.pyplot as plt
import json
import sys
import os

def read_hadoop_output(file_path):
    nodes = []
    edges = []
    
    reading_nodes = False
    reading_edges = False
    
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line.endswith('NODES_START'):
                reading_nodes = True
                continue
            elif line.endswith('NODES_END'):
                reading_nodes = False
                continue
            elif line.endswith('EDGES_START'):
                reading_edges = True
                continue
            elif line.endswith('EDGES_END'):
                reading_edges = False
                continue
                
            if reading_nodes and line:
                try:
                    # Remove the key part (e.g., "-1    ") from the line
                    json_str = line.split('\t')[1]
                    node_data = json.loads(json_str)
                    nodes.append(node_data)
                except:
                    continue
                    
            if reading_edges and line:
                try:
                    # Remove the key part from the line
                    json_str = line.split('\t')[1]
                    edge_data = json.loads(json_str)
                    edges.append(edge_data)
                except:
                    continue
    
    return nodes, edges

def create_graph_visualization(nodes, edges, output_file):
    G = nx.Graph()
    
    # Add nodes with their attributes
    for node in nodes:
        G.add_node(node['id'], node_type=node['type'])
    
    # Add edges
    for edge in edges:
        G.add_edge(edge['source'], edge['target'])
    
    # Set up the plot
    plt.figure(figsize=(12, 8))
    
    # Create a layout for the graph
    pos = nx.spring_layout(G, k=1, iterations=50)
    
    # Draw normal nodes
    normal_nodes = [n for n in G.nodes() if G.nodes[n]['node_type'] == 'normal']
    nx.draw_networkx_nodes(G, pos, nodelist=normal_nodes, 
                          node_color='lightblue', 
                          node_size=500)
    
    # Draw dominating nodes
    dominating_nodes = [n for n in G.nodes() if G.nodes[n]['node_type'] == 'dominating']
    nx.draw_networkx_nodes(G, pos, nodelist=dominating_nodes, 
                          node_color='red', 
                          node_size=700)
    
    # Draw edges
    nx.draw_networkx_edges(G, pos)
    
    # Add labels to nodes
    labels = {node: str(node) for node in G.nodes()}
    nx.draw_networkx_labels(G, pos, labels)
    
    plt.title('Graph Visualization (Red: Dominating Nodes, Blue: Normal Nodes)')
    plt.axis('off')
    
    # Save the plot
    plt.savefig(output_file, format='png', dpi=300, bbox_inches='tight')
    plt.close()

def main():
    if len(sys.argv) != 3:
        print("Usage: python visualize.py <hadoop_output_file> <output_png_file>")
        sys.exit(1)
        
    hadoop_output = sys.argv[1]
    output_png = sys.argv[2]
    
    nodes, edges = read_hadoop_output(hadoop_output)
    create_graph_visualization(nodes, edges, output_png)
    print(f"Graph visualization saved to {output_png}")

if __name__ == "__main__":
    main() 