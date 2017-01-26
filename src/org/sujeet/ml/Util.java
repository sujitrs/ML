package org.sujeet.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.feature.ChiSqSelector;
import org.apache.spark.mllib.feature.ChiSqSelectorModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Dataset;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class Util {
	static final Logger logger = LogManager.getLogger(Util.class.getName());
	/**
	 * 1) Import data
	 * */
	public static JavaRDD<Vector> loadData(JavaSparkContext jsc,  String path){
		    JavaRDD<String> data = jsc.textFile(path);
		    NslMap.init();
		    System.out.println(NslMap.hm);
		    
		    JavaRDD<Vector> parsedData = data.map(
		      new Function<String, Vector>() {
		        /**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				public Vector call(String s) {
		        	//int mapVal=0;
		        	ModelMap[] val;
		          String[] sarray = s.split(",");
		          double[] values = new double[sarray.length];
		          for (int i = 0; i < sarray.length; i++) {
		        	  logger.debug("sarray["+i+"]="+sarray[i]+",");
		        	  
		        	if((NslMap.hm.containsKey(i))){
		        		val=(ModelMap[]) NslMap.hm.get(i);
		        		for(int j=0;j<val.length;j++){
		        			if(val[j].getStrVal().equals(sarray[i])){
		        				sarray[i]=String.valueOf(val[j].getIntVal());
		        			}
		        		}
		        		
		        	}  
		            values[i] = Double.parseDouble(sarray[i]);
		            logger.debug("Converted value="+values[i]);
		          } logger.debug("");
		          return Vectors.dense(values);
		        }
		      }
		    );
		    return parsedData;
		   
	}

	
	public static JavaRDD<LabeledPoint> loadLabeledData(JavaSparkContext jsc,  String path){
	    JavaRDD<String> data = jsc.textFile(path);
	    NslMap.init();
	    logger.debug(NslMap.hm);
	    
	    JavaRDD<LabeledPoint> parsedData = data.map(
	      new Function<String, LabeledPoint>() {
	        /**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public LabeledPoint call(String s) {
	        	//int mapVal=0;
				ModelMap[] val;
	          String[] sarray = s.split(",");
	          double[] values = new double[sarray.length];
	          double label = 0;
	          for (int i = 0; i < sarray.length; i++) {
	        	  //System.out.print("sarray["+i+"]="+sarray[i]+",");
	        	  
	        	if((NslMap.hm.containsKey(i))){
	        		val=(ModelMap[]) NslMap.hm.get(i);
	        		for(int j=0;j<val.length;j++){
	        			if(val[j].getStrVal().equals(sarray[i])){
	        				sarray[i]=String.valueOf(val[j].getIntVal());
	        			}
	        		}
	        		
	        	}  
	            
	            if(i==41){
	            	label=Double.parseDouble(sarray[i]);
	            }
	            else if(i==42){
	            	// Skip the column
	            }else{
	            	values[i] = Double.parseDouble(sarray[i]);
		            //System.out.print("Converted value="+values[i]);	
	            }
	            	
	          } //System.out.println("");
	          return new LabeledPoint(label, Vectors.dense(values));
	        }
	      }
	    );
	    return parsedData;
	   
}
	
	
	public static JavaRDD<LabeledPoint> feaureSelection(JavaRDD<LabeledPoint> points ){
		// Discretize data in 16 equal bins since ChiSqSelector requires categorical features
	    // Although features are doubles, the ChiSqSelector treats each unique value as a category
	    JavaRDD<LabeledPoint> discretizedData = points.map(
	      new Function<LabeledPoint, LabeledPoint>() {
	        @Override
	        public LabeledPoint call(LabeledPoint lp) {
	          final double[] discretizedFeatures = new double[lp.features().size()];
	          for (int i = 0; i < lp.features().size(); ++i) {
	            discretizedFeatures[i] = Math.floor(lp.features().apply(i) / 16);
	          }
	          return new LabeledPoint(lp.label(), Vectors.dense(discretizedFeatures));
	        }
	      }
	    );

	    // Create ChiSqSelector that will select top 15 of 40 features
	    ChiSqSelector selector = new ChiSqSelector(BuildModel.NO_OF_FEATURES_TO_BE_SELECTED);
	    // Create ChiSqSelector model (selecting features)
	    final ChiSqSelectorModel transformer = selector.fit(discretizedData.rdd());
	    // Filter the top 15 features from each feature vector
	    JavaRDD<LabeledPoint> filteredData = discretizedData.map(
	      new Function<LabeledPoint, LabeledPoint>() {
	        @Override
	        public LabeledPoint call(LabeledPoint lp) {
	          return new LabeledPoint(lp.label(), transformer.transform(lp.features()));
	        }
	      }
	    );
	    // $example off$

	    logger.debug("filtered data: "+filteredData.count());
	    return filteredData;
	    /*filteredData.foreach(new VoidFunction<LabeledPoint>() {
	      @Override
	      public void call(LabeledPoint labeledPoint) throws Exception {
	        System.out.println(labeledPoint.toString());
	      }
	    });*/	
	}
	
	public static BinaryClassificationMetrics logisticRegression( SparkContext sc, String modelName,JavaRDD<LabeledPoint> training, JavaRDD<LabeledPoint> test){
		
		final LogisticRegressionModel model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(training.rdd());
	    
			    // Compute raw scores on the test set.
			    JavaRDD<Tuple2<Object, Object>> predictionAndLabels= test.map(
			      new Function<LabeledPoint, Tuple2<Object, Object>>() {
			        public Tuple2<Object, Object> call(LabeledPoint p) {
			          Double prediction = model.predict(p.features());
			          return new Tuple2<Object, Object>(prediction, p.label());
			        }
			      }
			    );
			    
			    model.save(sc, BuildModel.PATH_FOR_SAVING_MODEL+modelName);
			    BinaryClassificationMetrics  metrics= new BinaryClassificationMetrics(predictionAndLabels.rdd());
			    
			    return metrics;
	}
	
	public static BinaryClassificationMetrics DecisionTree(SparkContext sc, String modelName, JavaRDD<LabeledPoint> training, JavaRDD<LabeledPoint> test){
		
		Integer numClasses = 2;// Two CLass
	    Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
	    String impurity = "gini";//"entropy";//"gini";
	    Integer maxDepth = 5;
	    Integer maxBins = 32;

	    // Train a DecisionTree model for classification.
	    final DecisionTreeModel model = DecisionTree.trainClassifier(training, numClasses,
	      categoricalFeaturesInfo, impurity, maxDepth, maxBins);

	    // Evaluate model on test instances and compute test error
	    /*JavaPairRDD<Double, Double> predictionAndLabel = test.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
	        @Override
	        public Tuple2<Double, Double> call(LabeledPoint p) {
	          return new Tuple2<>(model.predict(p.features()), p.label());
	        }
	      });*/
	    
	    JavaRDD<Tuple2<Object, Object>> predictionAndLabels= test.map(
			      new Function<LabeledPoint, Tuple2<Object, Object>>() {
			        public Tuple2<Object, Object> call(LabeledPoint p) {
			          Double prediction = model.predict(p.features());
			          return new Tuple2<Object, Object>(prediction, p.label());
			        }
			      }
			    );
	    
	   /* Double testErr =
	      1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
	        @Override
	        public Boolean call(Tuple2<Double, Double> pl) {
	          return !pl._1().equals(pl._2());
	        }
	      }).count() / test.count();*/
	    model.save(sc, BuildModel.PATH_FOR_SAVING_MODEL+modelName);
	    return new BinaryClassificationMetrics(predictionAndLabels.rdd());
	    
		
	}
	
	public static BinaryClassificationMetrics RandomForest(SparkContext sc, String modelName,JavaRDD<LabeledPoint> training, JavaRDD<LabeledPoint> test)
	{
		 Integer numClasses = 2;
		    HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
		    Integer numTrees = 100; // Use more in practice.
		    String featureSubsetStrategy = "sqrt"; // Let the algorithm choose.
		    String impurity = "entropy";
		    Integer maxDepth = 5;
		    Integer maxBins = 32;
		    Integer seed = 12345;

		    final RandomForestModel model = RandomForest.trainClassifier(training, numClasses,
		      categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,
		      seed);

		    // Evaluate model on test instances and compute test error
		   /* JavaPairRDD<Double, Double> predictionAndLabel =
		    		test.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
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
		System.out.println("Learned classification forest model:\n" + model.toDebugString());*/
		    
		    JavaRDD<Tuple2<Object, Object>> predictionAndLabels= test.map(
				      new Function<LabeledPoint, Tuple2<Object, Object>>() {
				        public Tuple2<Object, Object> call(LabeledPoint p) {
				          Double prediction = model.predict(p.features());
				          return new Tuple2<Object, Object>(prediction, p.label());
				        }
				      }
				    );
		    model.save(sc, BuildModel.PATH_FOR_SAVING_MODEL+modelName);
		    return new BinaryClassificationMetrics(predictionAndLabels.rdd());
	}
	
	public static BinaryClassificationMetrics SVMwithSGD(SparkContext sc, String modelName,JavaRDD<LabeledPoint> training, JavaRDD<LabeledPoint> test)
	{
	    // Run training algorithm to build the model.
	    int numIterations = 100;
	    final SVMModel model = SVMWithSGD.train(training.rdd(), numIterations);

	    // Clear the default threshold.
	    model.clearThreshold();
	    
	    // Compute raw scores on the test set.
	    JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(
	      new Function<LabeledPoint, Tuple2<Object, Object>>() {
	        public Tuple2<Object, Object> call(LabeledPoint p) {
	          Double score = model.predict(p.features());
	          return new Tuple2<Object, Object>(score, p.label());
	        }
	      }
	    );

	    // Get evaluation metrics.
	    model.save(sc, BuildModel.PATH_FOR_SAVING_MODEL+modelName);
	    return new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
	}
	
}
