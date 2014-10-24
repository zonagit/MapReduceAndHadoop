package matrixmultiply;
import org.apache.hadoop.conf.Configuration;

/**
 * 
 * based on tutorial at
 *	 
 *	www.norstad.org/matrix-multiply/
 *
 *and Chapter 2 of Mining of Massive Datasets
 *book.
 */
public class OnePassMatrixMultiplyMain 
{	
	public static boolean debug = false;
	public static void main(String[] args) throws Exception 
	{
		try
		{
			//Create 2 dense random matrices and write them
			//down to 2 different files
			int I = new Integer(args[0]);
			int K = new Integer(args[1]);
			int J = new Integer(args[2]);
			debug = new Boolean(args[3]);
		
			MatrixMultiplyUtils.init();
			MatrixMultiplyUtils.buildRandomMatrices(I, K, J);
			
			//check file read/write works ok
			if (debug)
			{
				double[][] M = MatrixMultiplyUtils.readMatrix(I,K,MatrixMultiplyUtils.INPUT_DIR_PATH,"M");
				double[][] N = MatrixMultiplyUtils.readMatrix(K,J,MatrixMultiplyUtils.INPUT_DIR_PATH,"N");
				System.out.println("M from file versus M computed");
				for (int i =0;i<I;i++)
				{
					for (int k=0;k<K;k++)
						System.out.println(M[i][k]+" "+MatrixMultiplyUtils.M[i][k]);
				}
				System.out.println("N from file versus N computed");
				for (int k=0;k<K;k++)
					for (int j=0;j<J;j++)
						System.out.println(N[k][j] + " "+MatrixMultiplyUtils.N[k][j]);
			}
			//Run Map reduce
			Configuration confMain = MatrixMultiplyUtils.conf;
			confMain.set("I", I + "");
			confMain.set("K", K +"");
			confMain.set("J", J + "");
			confMain.set("inputPathM", MatrixMultiplyUtils.INPUT_DIR_PATH + "M");
			confMain.set("inputPathN", MatrixMultiplyUtils.INPUT_DIR_PATH + "N");
			confMain.set("outputPath", MatrixMultiplyUtils.OUTPUT_DIR_PATH);				
			confMain.set("debug", debug + "");
			OnePassMatrixMultiply.run(confMain);
			
			//Check map reduce produces the right
			//matrix multiply
			if (debug)
			{
				System.out.println("Comparing normal matrix multiplication with map reduce result");
				MatrixMultiplyUtils.checkAnswer(MatrixMultiplyUtils.M, MatrixMultiplyUtils.N, I, K, J);
			}
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}
	

}
