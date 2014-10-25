package matrixmultiply;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;

/**
 * 
 * Converts from Hadoop's SequenceFileFormat to binary file format 
 * and the other way
 * Args:
 */
public class ConversionMain {
	static String INPUT_DIR_PATH = "/opt/hadoop-2.5.1/share/input/";
	static String input_filename,output_filename;
	static Configuration conf;
	static FileSystem fs;
	static int numrows,numcols;
	static boolean debug = false;
	
	@SuppressWarnings("deprecation")
	private static void readFromSequenceWriteToBin() throws IOException
	{
		Path path = new Path(input_filename);
		
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		MatrixMultiply.IndexPair indexPair = new MatrixMultiply.IndexPair();
		
		DoubleWritable element = new DoubleWritable();
		//read the last line of the file to figure out
		//the numrows and numcols which go in the first line
		//of the binary file		
		while (reader.next(indexPair, element))
		{
			
		}
		int numrows = indexPair.row + 1;
		int numcols = indexPair.col + 1;
		reader.close();
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(output_filename)));
		DataOutputStream data_out = new DataOutputStream(
				new BufferedOutputStream(
						new FileOutputStream(
								new File(output_filename))));
		reader = new SequenceFile.Reader(fs, path, conf);
//		data_out.writeChars(numrows + " " + numcols + "\n");
		bw.write(numrows + " " + numcols + "\n");
		while (reader.next(indexPair, element))
		{
//			data_out.writeDouble(element.get());
			bw.write(element.get()+"\n");
		}
//		data_out.flush();
		data_out.close();
		bw.close();
		reader.close();
	}
	
	
	@SuppressWarnings("deprecation")
	private static void readFromBinWriteToSequence() throws IOException
	{
		Path path = new Path(output_filename);
		DataInputStream data_in = new DataInputStream(
				new BufferedInputStream(
						new FileInputStream(
								new File(input_filename))));
		BufferedReader br = new BufferedReader(new FileReader(new File(input_filename)));
//		String header = data_in.readLine();
		String header = br.readLine();
		numrows = Integer.parseInt(header.split(" ")[0]);
		numcols = Integer.parseInt(header.split(" ")[1]);
		
		SequenceFile.Writer writer = 
				SequenceFile.createWriter(fs, conf, path,
						MatrixMultiply.IndexPair.class,
						DoubleWritable.class, 
						SequenceFile.CompressionType.NONE);
	
		MatrixMultiply.IndexPair indexPair = new MatrixMultiply.IndexPair();
		DoubleWritable element = new DoubleWritable();
		double value;
		for (int i=0;i<numrows;i++) 
		{
			for (int j=0;j<numcols;j++)
			{
//				value = data_in.readDouble();
				value = Double.parseDouble(br.readLine());
				//matrix is dense and it will never 
				//have 0 entries so no need for check
				indexPair.row = i;
				indexPair.col = j;
				
				element.set(value);
				writer.append(indexPair, element);
			}
		}
		data_in.close();
		writer.close();
	}
	
	public static void main(String[] args) throws IOException {
		boolean toBinary = Boolean.parseBoolean(args[0]);
		if (args.length == 5)
			INPUT_DIR_PATH = args[4];
		input_filename = INPUT_DIR_PATH +  args[1];
		output_filename = INPUT_DIR_PATH + args[2];
		conf = new Configuration();
		fs = FileSystem.get(conf);
		if (args.length == 4)
			debug = Boolean.parseBoolean(args[3]);
		if (debug)
		{
			test();
			return;
		}
		if (toBinary)
		{			
			readFromSequenceWriteToBin();
		}
		else
		{
			readFromBinWriteToSequence();			
		}
	}

	private static void test() throws IOException
	{
		//create a matrix
		MatrixMultiplyUtils.init();
		MatrixMultiplyUtils.buildRandomMatrices(4, 3, 2);
		double M[][] = MatrixMultiplyUtils.readMatrix(4, 3, INPUT_DIR_PATH, "M");
		input_filename = INPUT_DIR_PATH + "M";
		output_filename = INPUT_DIR_PATH + "Mbin";
		readFromSequenceWriteToBin();
		input_filename = INPUT_DIR_PATH + "Mbin";
		output_filename = INPUT_DIR_PATH + "Mseq";
		readFromBinWriteToSequence();
		double Mseq[][] = MatrixMultiplyUtils.readMatrix(4, 3, INPUT_DIR_PATH, "Mseq");
		for (int i=0;i<4;i++)
		{
			for (int j=0;j<3;j++)
			{
				if (Math.round(M[i][j]) !=Math.round(Mseq[i][j]))
				{
					System.out.println("Mismatch at i,j=" + i + " " + j + " values " + M[i][j]+ " versus "+Mseq[i][j]);
				}
				else
				{
					System.out.println("Match at i,j=" + i + " " + j + " values " + M[i][j]+ " versus "+Mseq[i][j]);
				}
			}
		}
	}
}
