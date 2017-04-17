package org.sujeet.ml.analyse;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.sujeet.ml.BuildModel;
//import org.apache.spark.mllib.linalg;
import org.sujeet.util.PostgreSQLJDBC;


public class DtAnalyser /*implements Analyse*/ implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static DecisionTreeModel DTmodel;
	static LogisticRegressionModel LRmodel;
	static RandomForestModel RFmodel;
	static SVMModel SvmModel;
	
	
	//@Override
	public boolean processAndPredict(String input) {
		
		String[] sarray = input.split(",");
		double[] values = new double[sarray.length];
		for (int i = 0; i < sarray.length; i++) {
			values[i] = Double.parseDouble(sarray[i]);
		}
		Vector features=Vectors.dense(values);
		
		Double DTprediction=DTmodel.predict(features);
		Double LRprediction=LRmodel.predict(features);
		//Double SVMprediction=SvmModel.predict(features);
		Double RFprediction=RFmodel.predict(features);
		System.out.println("DT::"+DTprediction);
		System.out.println("LR::"+LRprediction);
		//System.out.println("SVM::"+SVMprediction);
		System.out.println("RF::"+RFprediction);
		System.out.println(""+(DTprediction+LRprediction+	RFprediction));
		boolean finalPrediction=(DTprediction+LRprediction+RFprediction)>=2.0?true:false;
		PostgreSQLJDBC.saveDatasetAndResults(input, RFprediction, DTprediction,LRprediction,(finalPrediction==true?1.0:0.0));
		
		return finalPrediction;
		
	}

	//@Override
	

	
	public boolean report() {
		// TODO Auto-generated method stub
		return false;
	}

	public static boolean init(JavaSparkContext jsc) {
		// TODO Auto-generated method stub
		LRmodel	=LogisticRegressionModel.load(jsc.sc(),".\\resource\\models\\FullDataLR_13");
		DTmodel	=DecisionTreeModel.load(jsc.sc(),".\\resource\\models\\FullDataDT_13");
		RFmodel	=RandomForestModel.load(jsc.sc(),".\\resource\\models\\FullDataRF_13");
		SvmModel=SVMModel.load(jsc.sc(),".\\resource\\models\\FullDataSVM_13");
		PostgreSQLJDBC.init();
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
	    
	    DtAnalyser.init(jsc);
	    dt.processAndPredict(input);
	    System.out.println(dt.processAndPredict(input));
	}
	    
}
