import java.awt.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

import mpi.MPI;



public class MPJPageRank {

	// adjacency matrix read from file
	private HashMap<Integer, ArrayList<Integer>> adjMatrix = new HashMap<Integer, ArrayList<Integer>>();

	private ArrayList<NodeLinks> adjMatrixList = new ArrayList<NodeLinks>();

	private HashMap<Integer,Double> urlRankvalue = new HashMap<Integer,Double>();
	// input file name
	private String inputFile = "";
	// output file name
	private String outputFile = "";
	// number of iterations
	private int iterations = 10;
	// damping factor
	private double df = 0.85;
	// number of URLs
	private int size = 0;
	// calculating rank values
	private HashMap<Integer, Double> rankValues = new HashMap<Integer, Double>();

	public void parseArgs(String[] args) {
		if(args == null || args.length != 7){
			System.out.print("Java  MPJPageRank    pagerank.input   pagerank.output   <number of iterations>  <dampingFactor> ");
			System.exit(0);
		}
		inputFile = args[3].trim();
		outputFile = args[4].trim();
		df = Double.parseDouble(args[5].trim());
		iterations = Integer.parseInt(args[6].trim());
	}


	public void loadInput() throws IOException {
		FileReader fileReader = new FileReader(inputFile);
		BufferedReader bufferReader = new BufferedReader(fileReader);
		String line;
		while ((line = bufferReader.readLine()) != null){
			// System.out.println(line);
			String[] urls = line.split(" ");
			ArrayList<Integer> connectUrlList = new ArrayList<Integer>();
			Integer key = Integer.parseInt(urls[0].trim());
			for(int i=1; i<urls.length; i++){
				connectUrlList.add(Integer.parseInt(urls[i].trim()));    		   
			}
			adjMatrix.put(key, connectUrlList);
			adjMatrixList.add(new NodeLinks(key, connectUrlList));
		}
		bufferReader.close();

	}

	public void printValues() throws IOException {

		TreeMap<Node, Double> rankMap = new TreeMap<Node, Double>();

		for(Integer key:urlRankvalue.keySet()){
			// System.out.println(rankMap.entrySet());
			Node keyNode = new Node(key, urlRankvalue.get(key)); 
			rankMap.put(keyNode,  1.0);	
			// System.out.println(key + " " + urlRankvalue.get(key));
		}

		File file = new File(this.outputFile);
		file.createNewFile();
		FileWriter fileWriter = new FileWriter(file);


		int i = 0;

		for(java.util.Map.Entry<Node, Double> entry : rankMap.entrySet()) {

			if (i++ <10) {
				System.out.println(entry.getKey().getNodeID() + " " + entry.getKey().getRank());
			}
			fileWriter.write("Page: " + entry.getKey().getNodeID() + " |  Rank: " + entry.getKey().getRank() + "\n");
		}


		fileWriter.flush();
		fileWriter.close();

	}


	public static void main(String args[]) throws Exception {

		String[] inputFiles = {"pagerank.input.100.0","pagerank.input.800.0","pagerank.input.10000.0","pagerank.input.1200.0","pagerank.input.200.0","pagerank.input.2000.0","pagerank.input.20000.0","pagerank.input.3000.0","pagerank.input.30000.0","pagerank.input.4000.0","pagerank.input.40000.0","pagerank.input.500.0","pagerank.input.5000.0","pagerank.input.6000.0","pagerank.input.7000.0","pagerank.input.800.0","pagerank.input.9000.0"};
		for(int inputNumber=0; inputNumber<inputFiles.length; inputNumber++){
		long startTime = System.nanoTime();	
		MPI.Init(args);
		int rank = MPI.COMM_WORLD.Rank();
		int size = MPI.COMM_WORLD.Size();
		MPJPageRank mpjPageRank = new MPJPageRank();
		Object[] objBuffer = new Object[1];
		// Sending Adjacent Matrix data to each process
		if(rank ==0){
			//mpjPageRank.parseArgs(args);
			mpjPageRank.inputFile = "input/" + inputFiles[inputNumber];
			System.out.println(inputFiles[inputNumber]);
			mpjPageRank.outputFile = "out.txt";
			mpjPageRank.iterations = 100;
			mpjPageRank.loadInput();
			// assuming size is a factor of total size of matrix - Have to change
			int chunkSize = mpjPageRank.adjMatrix.size()/(size-1);

			for (int i = 1; i < size; i++) {
				int start = (i-1)* chunkSize;
				int end = i*chunkSize -1;
				// If size not exactly divisible the below equation handles this case
				if(i== size-1)
					end = mpjPageRank.adjMatrix.size() -1;

				ArrayList<NodeLinks> localAdjChunk = new ArrayList<NodeLinks>();

				for(int k= start; k<=end; k++)
					localAdjChunk.add(mpjPageRank.adjMatrixList.get(k));
				objBuffer[0] = (Object)localAdjChunk;
				MPI.COMM_WORLD.Send(objBuffer, 0, 1, MPI.OBJECT, i, 1);
			}
		}else{
			// here adjMatrixList refers to local
			MPI.COMM_WORLD.Recv(objBuffer, 0, 1, MPI.OBJECT, 0, 1);
			// Process 0 adjMatrixList is entire data : In other process adjMatrixList is local chunk data
			mpjPageRank.adjMatrixList = (ArrayList<NodeLinks>) objBuffer[0];
			// System.out.println("Rank :" + rank);
			// for(int i=0; i<mpjPageRank.adjMatrixList.size(); i++)
			//	System.out.println("Source URL :" + mpjPageRank.adjMatrixList.get(i).nodeId );
		}

		//System.out.println("I am process with rank : " + rank + " size : "+ size);
		int[] iterBuff = new int[1];
		Object[] urlRankBuff = new Object[1];

		if(rank ==0){
			int totalNumberOfUrls = mpjPageRank.adjMatrix.size();
			for(Integer key : mpjPageRank.adjMatrix.keySet()){
				mpjPageRank.urlRankvalue.put(key, 1.00/totalNumberOfUrls);
			}

			iterBuff[0] = mpjPageRank.iterations;
			for(int processId =1; processId<size; processId++){
				MPI.COMM_WORLD.Send(iterBuff, 0, 1, MPI.INT, processId, 1);
				//	System.out.println("Iteration Sending :" + processId);
			}

		}else{
			MPI.COMM_WORLD.Recv(iterBuff, 0, 1, MPI.INT,0,1);
			//System.out.println("Iteration Recived :" + rank);
			mpjPageRank.iterations = iterBuff[0];
		}


		for(int countIter = 0; countIter < mpjPageRank.iterations; countIter++){
			if(rank ==0){
				urlRankBuff[0] = (Object)mpjPageRank.urlRankvalue;
				for(int processId = 1; processId < size; processId++ )
					MPI.COMM_WORLD.Send(urlRankBuff, 0, 1, MPI.OBJECT,processId,1);

				// Getting data an updating new rank values 
				MPI.COMM_WORLD.Recv(urlRankBuff, 0, 1, MPI.OBJECT,1,1);
				HashMap<Integer,Double> tempUrlRankvalue = (HashMap<Integer, Double>) urlRankBuff[0];

				for(int processId = 2; processId < size; processId++ ) {
					MPI.COMM_WORLD.Recv(urlRankBuff, 0, 1, MPI.OBJECT,processId,1);
					addHashMap((HashMap<Integer,Double>)urlRankBuff[0], tempUrlRankvalue);
				}


				for(Integer key :tempUrlRankvalue.keySet()){
					Double rankVal = tempUrlRankvalue.get(key);
					// System.out.println("Key: " + key + " value: " + rankVal);
					rankVal = mpjPageRank.df*rankVal+(1-mpjPageRank.df)/mpjPageRank.urlRankvalue.size();
					tempUrlRankvalue.put(key, rankVal);
				}
				mpjPageRank.urlRankvalue = tempUrlRankvalue;


			}else{
				//System.out.println("Came " + countIter);
				//System.out.println("Termination" + mpjPageRank.iterations);
				MPI.COMM_WORLD.Recv(urlRankBuff, 0, 1, MPI.OBJECT, 0, 1);
				mpjPageRank.urlRankvalue = (HashMap<Integer, Double>) urlRankBuff[0];
				// System.out.println("Rank :" + rank);
				// for(Integer url: mpjPageRank.urlRankvalue.keySet())
				// 	System.out.println("Key :" +url + " Value :" +  mpjPageRank.urlRankvalue.get(url));


				HashMap<Integer,Double> tempUrlRankvalue = new HashMap<Integer,Double>();
				for(Integer key1 : mpjPageRank.urlRankvalue.keySet())
					tempUrlRankvalue.put(key1, 0.0);
				for(NodeLinks nodeLinks : mpjPageRank.adjMatrixList){
					ArrayList<Integer>connectedUrls = nodeLinks.links;
					Double rankVal = mpjPageRank.urlRankvalue.get(nodeLinks.nodeId);
					if(connectedUrls.size() == 0){
						for(Integer key1 : mpjPageRank.urlRankvalue.keySet()){
							Double value = tempUrlRankvalue.get(key1)+rankVal/(mpjPageRank.urlRankvalue.size());
							tempUrlRankvalue.put(key1,value );
						}
					}else{
						for(Integer key1 : connectedUrls){
							tempUrlRankvalue.put(key1, tempUrlRankvalue.get(key1)+rankVal/connectedUrls.size());
						}
					}
				}
				urlRankBuff[0] = (Object) tempUrlRankvalue;
				//	System.out.println("sending iter " + countIter);
				MPI.COMM_WORLD.Send(urlRankBuff, 0, 1, MPI.OBJECT,0,1);
				//System.out.println("sent iter " + countIter);
			}





		}

		//System.out.println("Rank :" + rank + " :" + "finilize");
		MPI.Finalize();
		// End of for-loop
		//if(rank ==0)
			//mpjPageRank.printValues();
		if(rank ==0)
		mpjPageRank.totalRunTime(startTime);
	}
	}

	public static double totalRunTime(long startTime){
		long duration = System.nanoTime() - startTime;
		double seconds = ((double)duration / 1000000000);
		System.out.println("Runtime in Seconds : " + seconds);
		return seconds;
		
		
	}
	private static void addHashMap(HashMap<Integer, Double> partialHashMap,
			HashMap<Integer, Double> newURLRank) {
		// TODO Auto-generated method stub
		for(Integer key: newURLRank.keySet()){

			newURLRank.put(key, newURLRank.get(key) + partialHashMap.get(key));

		}

	}



}
