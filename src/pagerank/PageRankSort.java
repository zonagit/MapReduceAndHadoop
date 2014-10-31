package pagerank;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 * Map-Reduce job to sort the final iteration of pagerank
 * Map the key:rank to value:page
 * Hadoop will do the sorting on key for us. No need to 
 * implement a reducer.
 * Sorting is ascending (so last is the largest pagerank)
 */
public class PageRankSort 
{
	public static class SortMapper extends Mapper<LongWritable,Text,DoubleWritable, Text>
	{
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException
		{
			String[] pageAndRank = getPageAndRank(key,value);
		
			double drank =  Double.parseDouble(pageAndRank[1]);
		
			Text page = new Text(pageAndRank[0]);
			DoubleWritable rank = new DoubleWritable(drank);
			context.write(rank,page);
		}
		
		private String[] getPageAndRank(LongWritable key, Text value) throws CharacterCodingException 
		{
			String[] pageAndRank = new String[2];
			int tabPageIndex = value.find("\t");
			int tabRankIndex = value.find("\t", tabPageIndex +1);
			
			//no tab after rank (when there are no links)
			int end;
			if (tabRankIndex == -1) 
			{
				end = value.getLength() - (tabPageIndex + 1);
			}
			else
			{
				end = tabRankIndex - (tabPageIndex + 1);
			}
			
			pageAndRank[0] = Text.decode(value.getBytes(),0 , tabPageIndex);
			pageAndRank[1] = Text.decode(value.getBytes(), tabPageIndex + 1, end);
			
			return pageAndRank;
		}
	}
	
	public static boolean rankOrdering(Path inputPath,Path outputPath) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		
		Job rankOrdering = Job.getInstance(conf,"rankOrdering");
		rankOrdering.setJarByClass(PageRankSort.class);
		rankOrdering.setOutputKeyClass(DoubleWritable.class);
		rankOrdering.setOutputValueClass(Text.class);
		rankOrdering.setMapperClass(SortMapper.class);
		
		
		FileInputFormat.setInputPaths(rankOrdering, inputPath);
		FileOutputFormat.setOutputPath(rankOrdering, outputPath);
	
		rankOrdering.setInputFormatClass(TextInputFormat.class);
		rankOrdering.setOutputFormatClass(TextOutputFormat.class);
		
		return rankOrdering.waitForCompletion(true);
	}
}

