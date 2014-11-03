package pagerank;

import org.apache.hadoop.fs.Path;

/**
 * Computes spam mass. Needs 2 inputs
 * a) The location of the folder containing the last pagerank iteration
 * b) The location of the folder containing the last trust page
 * rank iteration
 * 
 * and the output folder with the sorted spam mass
 * 
 *
 */
public class SpamMassMain {

	public static void main(String[] args) throws IllegalArgumentException, Exception {
		String prDir = args[0];
		String trDir = args[1];
		String outDir = args[2];
			
		SpamMass.calcSpamMass(new Path(prDir), new Path(trDir),new Path(outDir));
		//SpamMass.sortSpamMass();
	}

}
