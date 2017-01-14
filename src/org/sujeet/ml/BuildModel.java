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
		    JavaRDD<LabeledPoint>[] splitsFullData = parsedDataFullData.randomSplit(new double[] {0.7, 0.3}, 11L);
		    JavaRDD<LabeledPoint> trainingFullData = splitsFullData[0].cache();
		    JavaRDD<LabeledPoint> testFullData = splitsFullData[1];

		    
		 // Split initial RDD into two... [60% training data, 40% testing data].
		    JavaRDD<LabeledPoint>[] splitsFeatureSelectedData = parsedDataFeatureSelected.randomSplit(new double[] {0.7, 0.3}, 11L);
		    JavaRDD<LabeledPoint> trainingFeatureSelectedData = splitsFeatureSelectedData[0].cache();
		    JavaRDD<LabeledPoint> testFeatureSelectedData = splitsFeatureSelectedData[1];
		    
		    
		    
		    
		/* // Run training algorithm to build the model.
		    final LogisticRegressionModel modelFullData = new LogisticRegressionWithLBFGS()
		      .setNumClasses(2)
		      .run(trainingFullData.rdd());

		    // Compute raw scores on the test set.
		    JavaRDD<Tuple2<Object, Object>> predictionAndLabelsFullData = testFullData.map(
		      new Function<LabeledPoint, Tuple2<Object, Object>>() {
		        public Tuple2<Object, Object> call(LabeledPoint p) {
		          Double prediction = modelFullData.predict(p.features());
		          return new Tuple2<Object, Object>(prediction, p.label());
		        }
		      }
		    );

		    // Get evaluation metrics.
		    MulticlassMetrics metricsFullData = new MulticlassMetrics(predictionAndLabelsFullData.rdd());
		    double accuracyFullData = metricsFullData.accuracy();
		    System.out.println("Full Data Accuracy = " + accuracyFullData);
		    //--------------------------------------------------------------------------------
		    // Run training algorithm to build the model.
		    final LogisticRegressionModel modelFeatureSelection = new LogisticRegressionWithLBFGS()
		      .setNumClasses(2)
		      .run(trainingFeatureSelectedData.rdd());

		    // Compute raw scores on the test set.
		    JavaRDD<Tuple2<Object, Object>> predictionAndLabelsFeatureSelection = testFeatureSelectedData.map(
		      new Function<LabeledPoint, Tuple2<Object, Object>>() {
		        public Tuple2<Object, Object> call(LabeledPoint p) {
		          Double prediction = modelFeatureSelection.predict(p.features());
		          return new Tuple2<Object, Object>(prediction, p.label());
		        }
		      }
		    );

		    // Get evaluation metrics.
		    MulticlassMetrics metricsFeatureSelection = new MulticlassMetrics(predictionAndLabelsFeatureSelection.rdd());
		    double accuracyFeatureSelection = metricsFeatureSelection.accuracy();
		    System.out.println("FeatureSelection Data Accuracy = " + accuracyFeatureSelection);
		    System.out.println("Full Data Accuracy = " + accuracyFullData);*/
		    
		    
// What is Dataset<Row> data = spark.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");
		//@ TODO 6) Partition and sample 
		//@ TODO 7) Use ‘Two-Class Logistic Regression” and “Boosted Decision Tree” on separate partitions 
		 // Set parameters.
		    //  Empty categoricalFeaturesInfo indicates all features are continuous.
		    // Code for Decision Tree Starts All Attributes
		  /*  Integer numClasses = 2;// Two CLass
		    Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
		    String impurity = "gini";//"entropy";//"gini";
		    Integer maxDepth = 5;
		    Integer maxBins = 32;

		    // Train a DecisionTree model for classification.
		    final DecisionTreeModel model = DecisionTree.trainClassifier(trainingFullData, numClasses,
		      categoricalFeaturesInfo, impurity, maxDepth, maxBins);

		    // Evaluate model on test instances and compute test error
		    JavaPairRDD<Double, Double> predictionAndLabel =
		    		testFullData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
		        @Override
		        public Tuple2<Double, Double> call(LabeledPoint p) {
		          return new Tuple2<>(model.predict(p.features()), p.label());
		        }
		      });
		    Double testErr =
		      1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
		        @Override
		        public Boolean call(Tuple2<Double, Double> pl) {
		          return !pl._1().equals(pl._2());
		        }
		      }).count() / testFullData.count();

		    System.out.println("Test Error: " + testErr);
		System.out.println("Learned classification tree model:\n" + model.toDebugString());*/

		/*// Code for Decision Tree Starts Selected Attributes
	    Integer numClasses = 2;// Two CLass
	    Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
	    String impurity = "gini";//"entropy";//"gini";
	    Integer maxDepth = 5;
	    Integer maxBins = 32;

	    // Train a DecisionTree model for classification.
	    final DecisionTreeModel model = DecisionTree.trainClassifier(trainingFeatureSelectedData, numClasses,
	      categoricalFeaturesInfo, impurity, maxDepth, maxBins);

	    // Evaluate model on test instances and compute test error
	    JavaPairRDD<Double, Double> predictionAndLabel =
	    		testFeatureSelectedData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
	        @Override
	        public Tuple2<Double, Double> call(LabeledPoint p) {
	          return new Tuple2<>(model.predict(p.features()), p.label());
	        }
	      });
	    Double testErr =
	      1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
	        @Override
	        public Boolean call(Tuple2<Double, Double> pl) {
	          return !pl._1().equals(pl._2());
	        }
	      }).count() / testFeatureSelectedData.count();

	    System.out.println("Test Error: " + testErr);
	System.out.println("Learned classification tree model:\n" + model.toDebugString());*/
		    
		    Integer numClasses = 2;
		    HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
		    Integer numTrees = 10; // Use more in practice.
		    String featureSubsetStrategy = "auto"; // Let the algorithm choose.
		    String impurity = "gini";
		    Integer maxDepth = 5;
		    Integer maxBins = 32;
		    Integer seed = 12345;

		    final RandomForestModel model = RandomForest.trainClassifier(trainingFullData, numClasses,
		      categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,
		      seed);

		    // Evaluate model on test instances and compute test error
		    JavaPairRDD<Double, Double> predictionAndLabel =
		    		testFullData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
		        @Override
		        public Tuple2<Double, Double> call(LabeledPoint p) {
		          return new Tuple2<>(model.predict(p.features()), p.label());
		        }
		      });
		    Double testErr =
		      1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
		        @Override
		        public Boolean call(Tuple2<Double, Double> pl) {
		          return !pl._1().equals(pl._2());
		        }
		      }).count() / testFullData.count();
		    System.out.println("Test Error: " + testErr);
		System.out.println("Learned classification forest model:\n" + model.toDebugString());

		//@ TODO 8) Tune parameters using metric for measuring performance for classification/regression 
		//@ TODO 9) Score Model by adding scored labels and scored possibilities 
		//@ TODO 10) Evaluate model using either of ROC(True Positive Rate Vs False Positive Rate)/Regression Vs Recall/LIFT (Number of True Positivie Vs Positive Rate) 
		//@ TODO 11) Compare performance and conclude which model to be used
		//@ TODO what is difference between test error and accuracy ? are they same it needs to be comparable
		//@ TODO Try Pearson corelation algo along with chisqrd
		
	}

}
