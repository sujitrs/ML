package org.sujeet.ml;

import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

public class BuildModel {
	
	public static void main(String[] args) {
		
		//@ TODO 1) Import data
		//@ TODO 2) Edit Metadata
		//@ TODO 3) Convert Indicator Values		 	
			SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaKMeansExample");
		    JavaSparkContext jsc = new JavaSparkContext(conf);

		    
		    // Load and parse data
		    String path = ".\\resource\\network_intrusion_detection_with_target.csv";
		    //JavaRDD<Vector> parsedData =org.sujeet.ml.Util.loadData(jsc, path);
		    JavaRDD<LabeledPoint> parsedDataFullData =org.sujeet.ml.Util.loadLabeledData(jsc, path);

		    parsedDataFullData.cache();
		    System.out.println("Data row:"+parsedDataFullData.count());
		    parsedDataFullData.saveAsTextFile(".\\resource\\parseddata"+new Date().getTime()+".txt");
		    //@ TODO 4) Select Columns in dataset based on  
			//@ TODO 5) Feature selection 
		    JavaRDD<LabeledPoint> parsedDataFeatureSelected=parsedDataFullData;     
		    org.sujeet.ml.Util.feaureSelection(parsedDataFeatureSelected);
		    //
		    
		 // Split initial RDD into two... [60% training data, 40% testing data].
		    JavaRDD<LabeledPoint>[] splitsFullData = parsedDataFullData.randomSplit(new double[] {0.6, 0.4}, 11L);
		    JavaRDD<LabeledPoint> trainingFullData = splitsFullData[0].cache();
		    JavaRDD<LabeledPoint> testFullData = splitsFullData[1];

		    
		 // Split initial RDD into two... [60% training data, 40% testing data].
		    JavaRDD<LabeledPoint>[] splitsFeatureSelectedData = parsedDataFeatureSelected.randomSplit(new double[] {0.6, 0.4}, 11L);
		    JavaRDD<LabeledPoint> trainingFeatureSelectedData = splitsFeatureSelectedData[0].cache();
		    JavaRDD<LabeledPoint> testFeatureSelectedData = splitsFeatureSelectedData[1];

// What is Dataset<Row> data = spark.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");
		//@ TODO 6) Partition and sample 
		//@ TODO 7) Use ‘Two-Class Logistic Regression” and “Boosted Decision Tree” on separate partitions 
		//@ TODO 8) Tune parameters using metric for measuring performance for classification/regression 
		//@ TODO 9) Score Model by adding scored labels and scored possibilities 
		//@ TODO 10) Evaluate model using either of ROC(True Positive Rate Vs False Positive Rate)/Regression Vs Recall/LIFT (Number of True Positivie Vs Positive Rate) 
		//@ TODO 11) Compare performance and conclude which model to be used
		
	}

}
