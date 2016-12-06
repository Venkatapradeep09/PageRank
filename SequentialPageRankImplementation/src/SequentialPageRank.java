import java.io.*;
import java.util.*;

public class SequentialPageRank {
	
	private HashMap<Integer, ArrayList<Integer>> adjMatrix = new HashMap<Integer, ArrayList<Integer>>();
	private HashMap<Integer,Double> urlRankvalue = new HashMap<Integer,Double>();
	private String inputFile = "";
	private String outputFile = "";
	private int iterations = 50;
	// damping factor
	private double df = 0.85;
	// number of URLs
	private int size = 0;
	private HashMap<Integer, Double> rankValues = new HashMap<Integer, Double>();

	// Initializes Variables
	public void parseArgs(String[] args) {
		if(args == null || args.length != 4){
			System.out.print("Java  SequentialPageRank    pagerank.input   pagerank.output   <number of iterations>  <dampingFactor> ");
			System.exit(0);
		}
		inputFile = args[0].trim();
		outputFile = args[1].trim();
		iterations = Integer.parseInt(args[2].trim());
		df = Double.parseDouble(args[3].trim());
	}

	public void loadInput() throws IOException {
		FileReader fileReader = new FileReader("input/"+inputFile);
		BufferedReader bufferReader = new BufferedReader(fileReader);
		String line;
		while ((line = bufferReader.readLine()) != null){
			String[] urls = line.split(" ");
			ArrayList<Integer> connectUrlList = new ArrayList<Integer>();
			Integer key = Integer.parseInt(urls[0].trim());
			for(int i=1; i<urls.length; i++){
				connectUrlList.add(Integer.parseInt(urls[i].trim()));    		   
			}
			adjMatrix.put(key, connectUrlList);
		}
		bufferReader.close();

	}

	public void calculatePageRank() {
		int totalNumberOfUrls = adjMatrix.size();
		for(Integer key : adjMatrix.keySet()){
			urlRankvalue.put(key, 1.00/totalNumberOfUrls);
		}
		for(int i=0; i<iterations; i++){
			HashMap<Integer,Double> tempUrlRankvalue = new HashMap<Integer,Double>();
			for(Integer key : adjMatrix.keySet()){
				ArrayList<Integer>connectedUrls = adjMatrix.get(key);
				Double rankVal = urlRankvalue.get(key);
				if(connectedUrls.size() == 0){
					for(Integer key1 : adjMatrix.keySet()){
							if(tempUrlRankvalue.containsKey(key1)){
								Double value = tempUrlRankvalue.get(key1)+rankVal/(totalNumberOfUrls);
								tempUrlRankvalue.put(key1,value );
							}else{
								tempUrlRankvalue.put(key1, rankVal/(totalNumberOfUrls));
							}
						}
				}else{
					for(Integer key1 : connectedUrls){
						if(tempUrlRankvalue.containsKey(key1)){
							tempUrlRankvalue.put(key1, tempUrlRankvalue.get(key1)+rankVal/connectedUrls.size());
						}else{
							tempUrlRankvalue.put(key1, rankVal/connectedUrls.size());
						}
					}
				}


			}
			for(Integer key :tempUrlRankvalue.keySet()){
				Double rankVal = tempUrlRankvalue.get(key);
				rankVal = df*rankVal+(1-df)/totalNumberOfUrls;
				tempUrlRankvalue.put(key, rankVal);
			}
			urlRankvalue = tempUrlRankvalue;
		}

	}

	public void printValues() throws IOException {
		
		TreeMap<Node, Double> rankMap = new TreeMap<Node, Double>();
				
		for(Integer key:urlRankvalue.keySet()){
			Node keyNode = new Node(key, urlRankvalue.get(key)); 
			rankMap.put(keyNode,  1.0);	
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
	
	public static double totalRunTime(long startTime){
		long duration = System.nanoTime() - startTime;
		double seconds = ((double)duration / 1000000000);
		System.out.println("Runtime in Seconds : " + seconds);
		return seconds;
		
		
	}
	
	public static void main(String[] args) throws IOException {
		
		
		
		//sequentialPR.parseArgs(args);
		String[] inputFiles = {"pagerank.input.100.0","pagerank.input.10000.0","pagerank.input.1200.0","pagerank.input.200.0","pagerank.input.2000.0","pagerank.input.20000.0","pagerank.input.3000.0","pagerank.input.30000.0","pagerank.input.4000.0","pagerank.input.40000.0","pagerank.input.500.0","pagerank.input.5000.0","pagerank.input.6000.0","pagerank.input.7000.0","pagerank.input.800.0","pagerank.input.9000.0"};
		
		for(int i=0; i<inputFiles.length; i++){
		
		long startTime = System.nanoTime();
		SequentialPageRank sequentialPR = new SequentialPageRank();
		sequentialPR.inputFile = inputFiles[i];
		System.out.println(sequentialPR.inputFile);
		sequentialPR.outputFile = "output.txt";
		sequentialPR.loadInput();
		sequentialPR.calculatePageRank();
		sequentialPR.totalRunTime(startTime);
//		sequentialPR.printValues();
		}
		
	}
}
