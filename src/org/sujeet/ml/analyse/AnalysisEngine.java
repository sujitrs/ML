package org.sujeet.ml.analyse;

import org.apache.spark.api.java.JavaSparkContext;

public interface AnalysisEngine {
	
	public boolean processAndPredict();
	public boolean report();
	public boolean init();
	boolean init(JavaSparkContext jsc);
	boolean processAndPredict(String input);
}
