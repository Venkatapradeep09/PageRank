import java.io.Serializable;
import java.util.ArrayList;

import mpi.MPI;

class NodeLinks implements Serializable{
	public NodeLinks(int nodeId,ArrayList<Integer> connectedLinks ){
		this.nodeId = nodeId;
		this.links = connectedLinks;
	}
	int nodeId;
	ArrayList<Integer> links = new ArrayList<Integer>();
}