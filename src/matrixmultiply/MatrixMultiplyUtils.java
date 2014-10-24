package matrixmultiply;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;


public class MatrixMultiplyUtils 
{
	public static final String INPUT_DIR_PATH = "/opt/hadoop-2.5.1/share/input/";
	public static final String TEMP_DIR_PATH = "/opt/hadoop-2.5.1/share/temp/";
	public static final String OUTPUT_DIR_PATH = "/opt/hadoop-2.5.1/share/output/";
	
	public static double[][] M;
	public static double[][] N;
	private static Random random = new Random();
	
	public static Configuration conf = new Configuration();
	private static FileSystem fs;
	
	public static void init()
	{
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("deprecation")
	public static double[][] readMatrix(int I,int J,String pth,String filename) throws IOException
	{
		double[][] matrix = new double[I][J];
		Path path = new Path(pth + filename);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		MatrixMultiply.IndexPair indexPair = new MatrixMultiply.IndexPair();
		
		DoubleWritable element = new DoubleWritable();
		while (reader.next(indexPair, element))
		{
			matrix[indexPair.row][indexPair.col]
					= element.get();
		}
		reader.close();
		
		return matrix;
	}
	
	@SuppressWarnings("deprecation")
	private static void writeMatrix(double[][] matrix,
			int I,int J,String filename) throws IOException
	{
		Path path = new Path(INPUT_DIR_PATH + filename);
		
		SequenceFile.Writer writer = 
				SequenceFile.createWriter(fs, conf, path,
						MatrixMultiply.IndexPair.class,
						DoubleWritable.class, 
						SequenceFile.CompressionType.NONE);
	
		MatrixMultiply.IndexPair indexPair = new MatrixMultiply.IndexPair();
		DoubleWritable element = new DoubleWritable();
		double value;
		for (int i=0;i<I;i++) 
		{
			for (int j=0;j<J;j++)
			{
				value = matrix[i][j];
				//matrix is dense and it will never 
				//have 0 entries so no need for check
				indexPair.row = i;
				indexPair.col = j;
				
				element.set(value);
				writer.append(indexPair, element);
			}
		}
		writer.close();
	}
	
	private static double[][] buildRandomMatrix(int I,int J)
	{
		double[][] matrix = new double[I][J];
		for (int i=0;i<I;i++)
		{
			for (int j=0;j<J;j++)
			{
				matrix[i][j] = 1.0 +100*random.nextDouble();
			}
		}
		
		return matrix;
	}
	
	public static void buildRandomMatrices(int I,int K,int J) throws IOException
	{		
		M = buildRandomMatrix(I,K);
		N = buildRandomMatrix(K,J);
		
		writeMatrix(M,I,K,"M");
		writeMatrix(N,K,J,"N");		
		
	}
	
	private static double[][] multiply(double[][] M,double[][]N,int I,int K,int J)
	{
		double[][] P = new double[I][J];
		double pij;
		for (int i=0;i<I;i++)
		{
			for (int j=0;j<J;j++)
			{
				pij = 0;
				for (int k=0;k<K;k++)
				{
					pij += M[i][k]*N[k][j];
				}
				P[i][j] = pij;
			}
		}
		return P;
	}
	
	public static void checkAnswer (double[][] M,double[][] N,int I,int K,int J) throws IOException
	{
		double[][] P = multiply(M,N,I,K,J);
		double[][] PbyMapReduce = readMatrix(I,J,OUTPUT_DIR_PATH,"part-r-00000");
		
		for (int i=0;i< I;i++)
		{
			for (int j=0; j<J;j++)
			{
				if ((P[i][j] - PbyMapReduce[i][j])>0.00001)
				{
					System.out.println("Wrong element at "+"i="+i+"j="+j);
					System.out.println("Correct value=" + P[i][j] + " doesnt match map reduce value "+ PbyMapReduce[i][j]);
				}
				else
				{
					System.out.println("Match at " + "i="+i+" j="+j);
					System.out.println("P[i][j]=" + P[i][j] + "P[i][j]MR=" + PbyMapReduce[i][j]);
				}
			}
		}
	}
}
