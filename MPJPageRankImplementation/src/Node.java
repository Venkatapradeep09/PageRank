public class Node implements Comparable<Node> {
	
	int nodeID;
	double rank;
	
	public Node(int nodeID, Double rank) {
		this.nodeID = nodeID;
		this.rank = rank;
	}
	
	public int getNodeID() {
		return this.nodeID;
	}
	
	public double getRank() {
		return this.rank;
	}
	
	public int compareTo(Node o) {
		// TODO Auto-generated method stub
		if (this.rank > o.rank) return -1;
		else if (this.rank == o.rank) return 1;
		else return 1;
	}

}