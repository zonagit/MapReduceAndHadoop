package pagerank;

import it.unimi.dsi.fastutil.io.BinIO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IdsToLabelsMain {

	public static void main(String[] args) throws ClassNotFoundException, IOException 
	{
		String basename = args[0];
		List<Integer> ids = new ArrayList<Integer>();
		for (int i=1;i< args.length;i++)
		{
			ids.add(Integer.parseInt(args[i]));
		}
		
		List<? extends CharSequence> node2Label = (List<? extends CharSequence>)BinIO.loadObject(basename +".fcl");
		
		for (int i=0;i<ids.size();i++)
		{
			System.out.println("node " + ids.get(i) + " Label "+node2Label.get(ids.get(i)));
		}
	}

}
