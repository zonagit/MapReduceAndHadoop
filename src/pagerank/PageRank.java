package pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank 
{
	private static boolean debug = false;
	private static boolean debug_iter = true;
	
	public static class Map
    extends Mapper<Text, Text, Text, Text> 
	{
		private Text outKey = new Text();
		private Text outValue = new Text();

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException 
		{
			//emit the input to preserve the graph structure
			context.write(key, value);

			Node node = Node.fromMR(value.toString());

			if(node.getAdjacentNodeNames() != null &&
					node.getAdjacentNodeNames().length > 0) 
			{
				double outboundPageRank = node.getPageRank() /
						(double)node.getAdjacentNodeNames().length;

				// go through all the nodes and propagate PageRank to them
				//
				for (int i = 0; i < node.getAdjacentNodeNames().length; i++) 
				{
					String neighbor = node.getAdjacentNodeNames()[i];
					outKey.set(neighbor);

					Node adjacentNode = new Node().setPageRank(outboundPageRank);

					outValue.set(adjacentNode.toString());
					if (debug)
					{
						System.out.println("  output -> K[" + outKey + "],V[" + outValue + "]");
					}
					try
					{
						context.write(outKey, outValue);
					}
					catch(Exception ex)
					{
						ex.printStackTrace();
					}
				}
			}
		}
	}
	
	public static class Reduce
    extends Reducer<Text, Text, Text, Text> 
	{
		public static enum Counter 
		{
		    CONV_DELTAS
		}
		public static final double CONVERGENCE_SCALING_FACTOR = 1000.0;
		public static final double DAMPING_FACTOR = 0.85;
		public static String CONF_NUM_NODES_GRAPH = "pagerank.numnodes";
		private int numberOfNodesInGraph;

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException 
				{
			numberOfNodesInGraph = context.getConfiguration().getInt(
					CONF_NUM_NODES_GRAPH, 0);
				}

		private Text outValue = new Text();

		public void reduce(Text key, Iterable<Text> values,
                     Context context)
      throws IOException, InterruptedException 
      {
			if (debug)
			{
				System.out.println("input -> K[" + key + "]");
			}
			double summedPageRanks = 0;
			Node originalNode = new Node();

			for (Text textValue : values) 
			{
				if (debug)
				{
					System.out.println("  input -> V[" + textValue + "]");
				}
				Node node = Node.fromMR(textValue.toString());

				if (node.containsAdjacentNodes()) 
				{
					// the original node
					//
					originalNode = node;
				}
				else 
				{
					summedPageRanks += node.getPageRank();
				}
			}

			double dampingFactor =
					((1.0 - DAMPING_FACTOR) / (double) numberOfNodesInGraph);

			double newPageRank =
					dampingFactor + (DAMPING_FACTOR * summedPageRanks);

			double delta = originalNode.getPageRank() - newPageRank;

			originalNode.setPageRank(newPageRank);

			outValue.set(originalNode.toString());

			if (debug)
			{
				System.out.println("  output -> K[" + key + "],V[" + outValue + "]");
			}
			context.write(key, outValue);
				

			int scaledDelta =
					Math.abs((int) (delta * CONVERGENCE_SCALING_FACTOR));

			if (debug)
			{
				System.out.println("Delta = " + scaledDelta);
			}
			try
			{
				context.getCounter(Counter.CONV_DELTAS).increment(scaledDelta);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
      }
	}
	
	 public static double calcPageRank(Path inputPath, Path outputPath, int numNodes)
		      throws Exception 
		      {
		 
		 Configuration conf = new Configuration();
		 conf.setInt(Reduce.CONF_NUM_NODES_GRAPH, numNodes);

		 Job job = new Job(conf);
		 job.setJarByClass(Main.class);
		 job.setMapperClass(Map.class);
		 job.setReducerClass(Reduce.class);
		 
		 job.setInputFormatClass(KeyValueTextInputFormat.class);

		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(Text.class);
		    
		 FileInputFormat.setInputPaths(job, inputPath);
		 FileOutputFormat.setOutputPath(job, outputPath);
		 
		 if (!job.waitForCompletion(true)) {
		      throw new Exception("Job failed");
		 }

		 long summedConvergence = job.getCounters().findCounter(
				 Reduce.Counter.CONV_DELTAS).getValue();
		 double convergence =
		        ((double) summedConvergence /
		            Reduce.CONVERGENCE_SCALING_FACTOR) /
		            (double) numNodes;

		 if (debug_iter)
		 {
			 System.out.println("======================================");
			 System.out.println("=  Num nodes:           " + numNodes);
			 System.out.println("=  Summed convergence:  " + summedConvergence);
			 System.out.println("=  Convergence:         " + convergence);
			 System.out.println("======================================");
		 }
		 return convergence;
		      }
}
