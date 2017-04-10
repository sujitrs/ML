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
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;
import org.sujeet.util.PostgreSQLJDBC;
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
		
		//@ TODO 1) Import data
		//@ TODO 2) Edit Metadata
		//@ TODO 3) Convert Indicator Values		 	
			SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaKMeansExample");
		    JavaSparkContext jsc = new JavaSparkContext(conf);
		    jsc.setLogLevel("ERROR");

		    //
		    // Load and parse data
		    //JavaRDD<Vector> parsedData =org.sujeet.ml.Util.loadData(jsc, path);
		    JavaRDD<LabeledPoint> parsedDataFullData =org.sujeet.ml.Util.loadLabeledData(jsc, PATH_FOR_LABELED_DATASET);

		    parsedDataFullData.cache();
		    PostgreSQLJDBC.init();
		    logger.debug("Data row:"+parsedDataFullData.count());
		    parsedDataFullData.saveAsTextFile(".\\resource\\parseddata_"+PostgreSQLJDBC.id+".txt");
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
		    
		    
		    
		    BinaryClassificationMetrics fullDataLrMetrics = Util.logisticRegression(jsc.sc(), "FullDataLR", trainingFullData, testFullData);
		    BinaryClassificationMetrics selectedDataLrMetrics = Util.logisticRegression(jsc.sc(), "SelDataLR",  trainingFeatureSelectedData, testFeatureSelectedData);
		    
		    BinaryClassificationMetrics fullDataDtMetrics =Util.DecisionTree(jsc.sc(), "FullDataDT", trainingFullData, testFullData);
		    BinaryClassificationMetrics selectedDataDtMetrics = Util.DecisionTree(jsc.sc(), "SelDataDT", trainingFeatureSelectedData, testFeatureSelectedData);
		    
		    BinaryClassificationMetrics fullDataRfMetrics =Util.RandomForest(jsc.sc(), "FullDataRF", trainingFullData, testFullData);
		    BinaryClassificationMetrics selectedDataRfMetrics = Util.RandomForest(jsc.sc(), "SelDataRF", trainingFeatureSelectedData, testFeatureSelectedData);
		    
		    BinaryClassificationMetrics fullDataSvmMetrics =Util.SVMwithSGD(jsc.sc(), "FullDataSVM", trainingFullData, testFullData);
		    BinaryClassificationMetrics selectedDataSvmMetrics = Util.SVMwithSGD(jsc.sc(), "SelDataSVM", trainingFeatureSelectedData, testFeatureSelectedData);
		    
		    
		    //JavaRDD<LabeledPoint> testData=new 
		    
		    logger.info("Area Under ROC");
		    logger.info("====================");
		    logger.info("1. Logistic Regression: All Features,"+fullDataLrMetrics.areaUnderROC());
		    logger.info("2. Logistic Regression: Selected Features, "+selectedDataLrMetrics.areaUnderROC());
		    logger.info("3. Decision Tree: All Features,"+fullDataDtMetrics.areaUnderROC());
		    logger.info("4. Decision Tree: Selected Feature, "+selectedDataDtMetrics.areaUnderROC());
		    logger.info("5. Random Forest: All Features,"+fullDataRfMetrics.areaUnderROC());
		    logger.info("6. Random Forest: Selected Feature ,"+selectedDataRfMetrics.areaUnderROC());
		    logger.info("7. SVM: All Features,"+fullDataSvmMetrics.areaUnderROC());
		    logger.info("8. SVM: Selected Feature ,"+selectedDataSvmMetrics.areaUnderROC());
		    
		    jsc.stop();
		
		//@ TODO 8) Tune parameters using metric for measuring performance for classification/regression 
		//@ TODO 9) Score Model by adding scored labels and scored possibilities 
		//@ TODO 10) Evaluate model using either of ROC(True Positive Rate Vs False Positive Rate)/Regression Vs Recall/LIFT (Number of True Positivie Vs Positive Rate) 
		//@ TODO 11) Compare performance and conclude which model to be used
		//@ TODO what is difference between test error and accuracy ? are they same it needs to be comparable
		//@ TODO Try Pearson corelation algo along with chisqrd
		
	}

}
