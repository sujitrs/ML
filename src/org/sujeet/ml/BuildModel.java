package org.sujeet.ml;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.util.MLUtils;

public class BuildModel {
	public static final int NO_OF_FEATURES_TO_BE_SELECTED=15;
	
	public static void main(String[] args) {
		
		//@ TODO 1) Import data
		//@ TODO 2) Edit Metadata
		//@ TODO 3) Convert Indicator Values		 	
			SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaKMeansExample");
		    JavaSparkContext jsc = new JavaSparkContext(conf);
		    jsc.setLogLevel("ERROR");

		    
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
		    parsedDataFeatureSelected=org.sujeet.ml.Util.feaureSelection(parsedDataFeatureSelected);
		    //
		    
		 // Split initial RDD into two... [60% training data, 40% testing data].
		    JavaRDD<LabeledPoint>[] splitsFullData = parsedDataFullData.randomSplit(new double[] {0.6, 0.4}, 11L);
		    JavaRDD<LabeledPoint> trainingFullData = splitsFullData[0].cache();
		    JavaRDD<LabeledPoint> testFullData = splitsFullData[1];

		    
		 // Split initial RDD into two... [60% training data, 40% testing data].
		    JavaRDD<LabeledPoint>[] splitsFeatureSelectedData = parsedDataFeatureSelected.randomSplit(new double[] {0.6, 0.4}, 11L);
		    JavaRDD<LabeledPoint> trainingFeatureSelectedData = splitsFeatureSelectedData[0].cache();
		    JavaRDD<LabeledPoint> testFeatureSelectedData = splitsFeatureSelectedData[1];
		    
		    BinaryClassificationMetrics fullDataLrMetrics = Util.logisticRegression(trainingFullData, testFullData);
		    BinaryClassificationMetrics selectedDataLrMetrics = Util.logisticRegression(trainingFeatureSelectedData, testFeatureSelectedData);
		    
		    BinaryClassificationMetrics fullDataDtMetrics =Util.DecisionTree(trainingFullData, testFullData);
		    BinaryClassificationMetrics selectedDataDtMetrics = Util.DecisionTree(trainingFeatureSelectedData, testFeatureSelectedData);
		    
		    BinaryClassificationMetrics fullDataRfMetrics =Util.RandomForest(trainingFullData, testFullData);
		    BinaryClassificationMetrics selectedDataRfMetrics = Util.RandomForest(trainingFeatureSelectedData, testFeatureSelectedData);
		    
		    BinaryClassificationMetrics fullDataSvmMetrics =Util.SVMwithSGD(trainingFullData, testFullData);
		    BinaryClassificationMetrics selectedDataSvmMetrics = Util.SVMwithSGD(trainingFeatureSelectedData, testFeatureSelectedData);
		    
		    
		    
		    
		    System.out.println("Area Under ROC");
		    System.out.println("====================");
		    System.out.println("1. Logistic Regression: All Features,"+fullDataLrMetrics.areaUnderROC());
		    System.out.println("2. Logistic Regression: Selected Features, "+selectedDataLrMetrics.areaUnderROC());
		    System.out.println("3. Decision Tree: All Features,"+fullDataDtMetrics.areaUnderROC());
		    System.out.println("4. Decision Tree: Selected Feature, "+selectedDataDtMetrics.areaUnderROC());
		    System.out.println("5. Random Forest: All Features,"+fullDataRfMetrics.areaUnderROC());
		    System.out.println("6. Random Forest: Selected Feature ,"+selectedDataRfMetrics.areaUnderROC());
		    System.out.println("7. SVM: All Features,"+fullDataSvmMetrics.areaUnderROC());
		    System.out.println("8. SVM: Selected Feature ,"+selectedDataSvmMetrics.areaUnderROC());
		    
		    jsc.stop();
		
		//@ TODO 8) Tune parameters using metric for measuring performance for classification/regression 
		//@ TODO 9) Score Model by adding scored labels and scored possibilities 
		//@ TODO 10) Evaluate model using either of ROC(True Positive Rate Vs False Positive Rate)/Regression Vs Recall/LIFT (Number of True Positivie Vs Positive Rate) 
		//@ TODO 11) Compare performance and conclude which model to be used
		//@ TODO what is difference between test error and accuracy ? are they same it needs to be comparable
		//@ TODO Try Pearson corelation algo along with chisqrd
		
	}

}
