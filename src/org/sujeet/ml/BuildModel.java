package org.sujeet.ml;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;

import org.sujeet.util.PostgreSQLJDBC;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class BuildModel {
	public static final int NO_OF_FEATURES_TO_BE_SELECTED=15;
	public static final String PATH_FOR_SAVING_MODEL=".\\resource\\models\\";
	public static final String PATH_FOR_LABELED_DATASET=".\\resource\\network_intrusion_detection_with_target.csv";
	public static final int LOGISTIC_REGRESSION=1;
	public static final int DECISION_TREE=2;
	public static final int RANDOM_FOREST=3;
	public static final int SVM_SDG=4;
	
	
	static final Logger logger = LogManager.getLogger(BuildModel.class.getName());
	
	public static void main(String[] args) {
		 	
			SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaKMeansExample");
		    JavaSparkContext jsc = new JavaSparkContext(conf);
		    jsc.setLogLevel("ERROR");

		    JavaRDD<LabeledPoint> parsedDataFullData =org.sujeet.ml.Util.loadLabeledData(jsc, PATH_FOR_LABELED_DATASET);

		    parsedDataFullData.cache();
		    PostgreSQLJDBC.init();
		    logger.debug("Data row:"+parsedDataFullData.count());
		    parsedDataFullData.saveAsTextFile(".\\resource\\parseddata_"+PostgreSQLJDBC.id+".txt");
		    JavaRDD<LabeledPoint> parsedDataFeatureSelected=parsedDataFullData;     
		    parsedDataFeatureSelected=org.sujeet.ml.Util.feaureSelection(parsedDataFeatureSelected);
		    
		 // Split initial RDD into two... [60% training data, 40% testing data].
		    JavaRDD<LabeledPoint>[] splitsFullData = parsedDataFullData.randomSplit(new double[] {0.6, 0.4}, 11L);
		    JavaRDD<LabeledPoint> trainingFullData = splitsFullData[0].cache();
		    JavaRDD<LabeledPoint> testFullData = splitsFullData[1];

		    
		 // Split initial RDD into two... [60% training data, 40% testing data].
		    JavaRDD<LabeledPoint>[] splitsFeatureSelectedData = parsedDataFeatureSelected.randomSplit(new double[] {0.6, 0.4}, 11L);
		    JavaRDD<LabeledPoint> trainingFeatureSelectedData = splitsFeatureSelectedData[0].cache();
		    JavaRDD<LabeledPoint> testFeatureSelectedData = splitsFeatureSelectedData[1];
		    
		    
		    
		    Util.logisticRegression(jsc.sc(), "FullDataLR", trainingFullData, testFullData);
		    Util.logisticRegression(jsc.sc(), "SelDataLR",  trainingFeatureSelectedData, testFeatureSelectedData);
		    
		    Util.DecisionTree(jsc.sc(), "FullDataDT", trainingFullData, testFullData);
		    Util.DecisionTree(jsc.sc(), "SelDataDT", trainingFeatureSelectedData, testFeatureSelectedData);
		    
		    Util.RandomForest(jsc.sc(), "FullDataRF", trainingFullData, testFullData);
		    Util.RandomForest(jsc.sc(), "SelDataRF", trainingFeatureSelectedData, testFeatureSelectedData);
		    
		    Util.SVMwithSGD(jsc.sc(), "FullDataSVM", trainingFullData, testFullData);
		    Util.SVMwithSGD(jsc.sc(), "SelDataSVM", trainingFeatureSelectedData, testFeatureSelectedData);
		    
		    
		    
		    jsc.stop();
		
		
	}

}
