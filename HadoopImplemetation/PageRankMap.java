public class PageRankMap extends Mapper<LongWritable, Text, LongWritable, Text> {


	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			int numUrls = context.getConfiguration().getInt("numUrls",1);
			String line = value.toString();
			StringBuffer sb = new StringBuffer();
			RankRecord rrd = new RankRecord(line);
			int sourceUrl, targetUrl;
			if (rrd.targetUrlsList.size()<=0){
				double rankValuePerUrl = rrd.rankValue/(double)numUrls;
				for (int i=0;i<numUrls;i++){
				context.write(new LongWritable(i), new Text(String.valueOf(rankValuePerUrl)));
				}
			} else {
			context.write(new LongWritable(rrd.sourceUrl), new Text(String.valueOf(rrd.targetUrlsRaw)));
				System.out.println( "rrd.targetUrlsRaw:" + rrd.sourceUrl + "  "+rrd.targetUrlsRaw);
				double rankValuePerUrl = rrd.rankValue/(double)rrd.targetUrlsList.size();
				for(Integer targetUrl1 : rrd.targetUrlsList){
					System.out.println( "rrd.targetUrl1:" + targetUrl1 );
					context.write(new LongWritable(targetUrl1), new Text(String.valueOf(rankValuePerUrl)));
				}
			} 

		

}
