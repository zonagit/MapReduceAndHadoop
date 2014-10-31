package pagerank;

import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * Based on Alex Holmes pagerank implementation available at github
 *
 */
public class PageRankMain 
{
	
	public static ImmutableGraph graph;
	public static int numNodes;
	
	public static void main (String[] args) throws Exception
	{
		String basename = args[0];
		String outputDir = args[1];
		
		loadGraph(basename);
		
		iterate(basename,outputDir);
		
	}
	
	public static void loadGraph(String basename) throws IOException
	{
		graph = ImmutableGraph.loadSequential( basename );
		numNodes = graph.numNodes();		
	}
	
	public static void iterate(String input, String output) throws Exception
	{
		Configuration conf = new Configuration();
		Path outputPath = new Path(output);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		outputPath.getFileSystem(conf).mkdirs(outputPath);
		
		Path inputPath = new Path(outputPath,input + ".txt");
		
		createInputFile(new Path(input), inputPath);
		
		int iter = 1;
		double desiredConvergence = 0.01;
		
		while (true) 
		{
			Path jobOutputPath =
					new Path(outputPath, String.valueOf(iter));

			System.out.println("======================================");
			System.out.println("=  Iteration:    " + iter);
			System.out.println("=  Input path:   " + inputPath);
			System.out.println("=  Output path:  " + jobOutputPath);
			System.out.println("======================================");
			
			if (PageRank.calcPageRank(inputPath, jobOutputPath, numNodes) <
					desiredConvergence) 
			{
				System.out.println(
		            "Convergence is below " + desiredConvergence +
		                ", we're done");
		        break;
			}
			inputPath = jobOutputPath;
			iter++;	
		}	
	}
	
	
	public static void createInputFile(Path file, Path targetFile) throws IOException
	{
		//initial page rank of all nodes--careful if numNodes is too large
		//this might be too small for double precision so I multiplied it 
		//by a fudge factor, it really doesnt matter what this initial
		//probabilities are, pagerank should converge to the same value
		double fudge_factor = 1.0;
		if (targetFile.toString().contains("enron"))
			fudge_factor = 1.0;
		else if (targetFile.toString().contains("dblp-2011"))
			fudge_factor = 100000.0;
		double initialPageRank = 1.0 * fudge_factor / (double) numNodes;
		initialPageRank = 0.25;
		NodeIterator ni = graph.nodeIterator();
		Configuration conf = new Configuration();
		FileSystem fs = file.getFileSystem(conf);

		OutputStream os = fs.create(targetFile);
		//int max = 10;
		while (ni.hasNext())
		{
			String nid = ni.nextInt() + "";
			String parts[] = new String[ni.outdegree()];
			int links[] = ni.successorArray();
			for (int idx=0;idx< ni.outdegree();idx++)
			{
				parts[idx] = links[idx] + "";
			}
			Node node = new Node()
		          .setPageRank(initialPageRank)
		          .setAdjacentNodeNames(
		              Arrays.copyOfRange(parts, 0, parts.length));
			///max--;
			IOUtils.write(nid + '\t' + node.toString() + '\n', os);
			//if (max<0)
			//	break;
		}
		os.close();
	}
}
