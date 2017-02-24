package org.sujeet.ml.analyse;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.sujeet.ml.BuildModel;
//import org.apache.spark.mllib.linalg;


public class DtAnalyser /*implements Analyse*/ implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static DecisionTreeModel model; 
	//@Override
	public boolean processAndPredict(String input) {
		// TODO Auto-generated method stub''
		String[] sarray = input.split(",");
		double[] values = new double[sarray.length];
		for (int i = 0; i < sarray.length; i++) {
			values[i] = Double.parseDouble(sarray[i]);
		}
		Vector features=Vectors.dense(values);
		Double prediction=model.predict(features);
		//System.out.println(prediction);
		return prediction==1.0?true:false;
	}

	//@Override
	public boolean report() {
		// TODO Auto-generated method stub
		return false;
	}

	public static boolean init(JavaSparkContext jsc) {
		// TODO Auto-generated method stub
		 model = DecisionTreeModel.load(jsc.sc(),
				 BuildModel.PATH_FOR_SAVING_MODEL+"FullDataDT");
		return true;
	}

	//@Override
	public boolean processAndPredict() {
		// TODO Auto-generated method stub
		return false;
	}

	//@Override
	public boolean init() {
		// TODO Auto-generated method stub
		return false;
	}

	
	public static void main(String[] args) {

	    SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaKMeansExample");
	    JavaSparkContext jsc = new JavaSparkContext(conf);
	    DtAnalyser dt=new DtAnalyser();
	    String input="0.0,2.0,7.0,0.0,8.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,29.0,0.0,0.0,0.0,0.0,1.0,0.0,1.0,3.0,127.0,1.0,0.0,1.0,0.25,0.0,0.0,0.0,0.0,0.0,0.0";
	    
	    dt.init(jsc);
	    dt.processAndPredict(input);
	}
	    
}
