package org.apache.spark.examples.mllib;

// $example on$
import java.util.LinkedList;
// $example off$

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
// $example on$
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
// $example off$

/**
 * Example for compute principal components on a 'RowMatrix'.
 */
public class JavaPCAExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("PCA Example");
    SparkContext sc = new SparkContext(conf);

    // $example on$
    double[][] array = {{1.12, 2.05, 3.12}, {5.56, 6.28, 8.94}, {10.2, 8.0, 20.5},{10.2, 8.0, 20.5},{10.2, 8.0, 20.5}};
    LinkedList<Vector> rowsList = new LinkedList<>();
    for (int i = 0; i < array.length; i++) {
      Vector currentRow = Vectors.dense(array[i]);
      rowsList.add(currentRow);
    }
    JavaRDD<Vector> rows = JavaSparkContext.fromSparkContext(sc).parallelize(rowsList);

    // Create a RowMatrix from JavaRDD<Vector>.
    RowMatrix mat = new RowMatrix(rows.rdd());

    // Compute the top 3 principal components.
    Matrix pc = mat.computePrincipalComponents(3);
    RowMatrix projected = mat.multiply(pc);
    // $example off$
    Vector[] collectPartitions = (Vector[])projected.rows().collect();
    System.out.println("Projected vector of principal component:");
    for (Vector vector : collectPartitions) {
      System.out.println("\t" + vector);
    }
    
  }
}