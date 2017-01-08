

//
package org.apache.spark.examples.mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

// $example on$
import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
// $example off$

/**
 * Example for SVMWithSGD. With GIT and more changs to come
 */
public class JavaSVMWithSGExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaSVMWithSGDExample");
    SparkContext sc = new SparkContext(conf);
    // $example on$
    String path = "data/mllib/sample_libsvm_data.txt";
    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path).toJavaRDD();

    // Split initial RDD into two... [60% training data, 40% testing data].
    JavaRDD<LabeledPoint> training = data.sample(false, 0.6, 11L);
    training.cache();
    JavaRDD<LabeledPoint> test = data.subtract(training);

    // Run training algorithm to build the model.
    int numIterations = 200;
    final SVMModel model = SVMWithSGD.train(training.rdd(), numIterations);

    // Clear the default threshold.
    model.clearThreshold();

    // Compute raw scores on the test set.
    JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(
      new Function<LabeledPoint, Tuple2<Object, Object>>() {
        /**
		 * 
		 */
		private static final long serialVersionUID = 2L;

		public Tuple2<Object, Object> call(LabeledPoint p) {
          Double score = model.predict(p.features());
          return new Tuple2<Object, Object>(score, p.label());
        }
      }
    );

    // Get evaluation metrics.
    BinaryClassificationMetrics metrics =
      new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
    double auROC = metrics.areaUnderROC();

    System.out.println("Area under ROC = " + auROC);
    long randomID=0;
    // Save and load model
    try{
    	model.save(sc, "target/tmp/javaSVMWithSGDModel");
    }
    catch(Exception ex){
    	randomID=new java.util.Random().nextInt(100);
    	model.save(sc, "target/tmp/javaSVMWithSGDModel_"+randomID);
    	System.out.println("Model Saved with ="+"target/tmp/javaSVMWithSGDModel_"+randomID);
    	
    }
    SVMModel sameModel = SVMModel.load(sc, "target/tmp/javaSVMWithSGDModel_"+randomID);
    // $example off$

    sc.stop();
  }
}
