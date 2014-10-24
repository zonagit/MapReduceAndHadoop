import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class MatrixMultiply 
{
	// Key class for the input sequence file
	public static class IndexPair implements WritableComparable
	{	
		public int row,col;
	
		@Override
		public void readFields(DataInput arg0) throws IOException 
		{
			row = arg0.readInt();
			col = arg0.readInt();
		}

		@Override
		public void write(DataOutput arg0) throws IOException 
		{
			arg0.writeInt(row);
			arg0.writeInt(col);
		}

		@Override
		public int compareTo(Object arg0) 
		{
			IndexPair other = (IndexPair)arg0;
			if (row < other.row)
			{
				return -1;
			}
			else if (row > other.row)
			{
				return 1;
			}
			if (col < other.col)
			{
				return -1;
			}
			else if (col > other.col)
			{
				return 1;
			}
			return 0;
		}
		
	}
	
}
