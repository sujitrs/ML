package org.sujeet.ml;

import org.apache.spark.SparkConf;
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
public class Util {
	
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
		        	  System.out.print("sarray["+i+"]="+sarray[i]+",");
		        	  
		        	if((NslMap.hm.containsKey(i))){
		        		val=(ModelMap[]) NslMap.hm.get(i);
		        		for(int j=0;j<val.length;j++){
		        			if(val[j].getStrVal().equals(sarray[i])){
		        				sarray[i]=String.valueOf(val[j].getIntVal());
		        			}
		        		}
		        		
		        	}  
		            values[i] = Double.parseDouble(sarray[i]);
		            System.out.print("Converted value="+values[i]);
		          } System.out.println("");
		          return Vectors.dense(values);
		        }
		      }
		    );
		    return parsedData;
		   
	}

	
	public static JavaRDD<LabeledPoint> loadLabeledData(JavaSparkContext jsc,  String path){
	    JavaRDD<String> data = jsc.textFile(path);
	    NslMap.init();
	    System.out.println(NslMap.hm);
	    
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
	
	
	public static void feaureSelection(JavaRDD<LabeledPoint> points ){
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
	    ChiSqSelector selector = new ChiSqSelector(15);
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

	    System.out.println("filtered data: "+filteredData.count());
	    /*filteredData.foreach(new VoidFunction<LabeledPoint>() {
	      @Override
	      public void call(LabeledPoint labeledPoint) throws Exception {
	        System.out.println(labeledPoint.toString());
	      }
	    });*/	
	}
	
	public static void dosomething(Dataset<Row> data){
		   // Index labels, adding metadata to the label column.
		    // Fit on whole dataset to include all labels in index.
		    StringIndexerModel labelIndexer = new StringIndexer()
		      .setInputCol("label")
		      .setOutputCol("indexedLabel")
		      .fit(data);
		    // Automatically identify categorical features, and index them.
		    // Set maxCategories so features with > 4 distinct values are treated as continuous.
		    VectorIndexerModel featureIndexer = new VectorIndexer()
		      .setInputCol("features")
		      .setOutputCol("indexedFeatures")
		      .setMaxCategories(4)
		      .fit(data);

		    // Split the data into training and test sets (30% held out for testing)
		    Dataset<Row>[] splits = data.randomSplit(new double[] {0.7, 0.3});
		    Dataset<Row> trainingData = splits[0];
		    Dataset<Row> testData = splits[1];

		    // Train a RandomForest model.
		    RandomForestClassifier rf = new RandomForestClassifier()
		      .setLabelCol("indexedLabel")
		      .setFeaturesCol("indexedFeatures");

		    // Convert indexed labels back to original labels.
		    IndexToString labelConverter = new IndexToString()
		      .setInputCol("prediction")
		      .setOutputCol("predictedLabel")
		      .setLabels(labelIndexer.labels());

		    // Chain indexers and forest in a Pipeline
		    Pipeline pipeline = new Pipeline()
		      .setStages(new PipelineStage[] {labelIndexer, featureIndexer, rf, labelConverter});

		    // Train model. This also runs the indexers.
		    PipelineModel model = pipeline.fit(trainingData);

		    // Make predictions.
		    Dataset<Row> predictions = model.transform(testData);

		    // Select example rows to display.
		    predictions.select("predictedLabel", "label", "features").show(5);

		    // Select (prediction, true label) and compute test error
		    MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
		      .setLabelCol("indexedLabel")
		      .setPredictionCol("prediction")
		      .setMetricName("accuracy");
		    double accuracy = evaluator.evaluate(predictions);
		    System.out.println("Test Error = " + (1.0 - accuracy));
	}
	
	
}
