package org.sujeet.ml.analyse;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;

import scala.Tuple2;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.sujeet.ml.BuildModel;
import org.sujeet.ml.report.ReportToKafka;
import org.sujeet.ml.report.ReportingEngine;
import org.sujeet.util.PostgreSQLJDBC;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 *
 * Usage: Analysis <zkQuorum> <group> <topics> <numThreads>
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * To run this example:
 * D:\_dev\kafka_2.11-0.10.1.0\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic streams-file-input < ids.txt
 * 
 *   `$ bin/run-example org.apache.spark.examples.streaming.JavaKafkaWordCount zoo01,zoo02, \
 *    zoo03 my-consumer-group topic1,topic2 1`
 *    localhost:2181 connect-local streams-file-input 1
 *    
 *    E:\_dev\kafka_2.11-0.10.1.0\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic streams-file-input < E:\_dev\kafka_2.11-0.10.1.0\ids.txt
 *    
 */

public final class Analysis implements AnalysisEngine {
  private static final Pattern SPACE = Pattern.compile(" ");
  private static final Pattern COMMA = Pattern.compile(",");
  static final Logger logger = LogManager.getLogger(Analysis.class.getName());
  private Analysis() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 4) {
      System.err.println("Usage: Analysis <zkQuorum> <group> <topics> <numThreads>");
      System.exit(1);
    }
    
    logger.info(":::::::::::::::::::::::::::::::::::::::");
    logger.info("Online Anomaly Detection Engine Started");
    logger.info(":::::::::::::::::::::::::::::::::::::::");

    SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("Analysis");
    // Create the context with 2 seconds batch size
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(5000));

    int numThreads = Integer.parseInt(args[3]);
    Map<String, Integer> topicMap = new HashMap<>();
    String[] topics = args[2].split(",");
    for (String topic: topics) {
      topicMap.put(topic, numThreads);
    }
    ReportingEngine report=new ReportToKafka();
    report.init();
    PostgreSQLJDBC.init();
    JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, args[0], args[1], topicMap);
    DtAnalyser.init(jssc.sparkContext());
    DtAnalyser dt=new DtAnalyser();
    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
      @Override
      public String call(Tuple2<String, String> tuple2) {
    	  if(dt.processAndPredict(tuple2._2())){
    		  logger.info("Anomaly Found for Stream: "+tuple2._2());
    		  report.reportAnomaly(tuple2._2());
    		  return "Anomaly Found for Stream: "+tuple2._2();
    	  }
    	  logger.info("Normal Stream:: "+tuple2._2());
    	  report.reportNormal(tuple2._2());
        return "Normal Stream: "+tuple2._2();
      }
    });
    //@ TODO Print in topic-Anomalies else Normal
    
    lines.print();
    lines.count().print();
    //lines.foreachRDD().;
    //System.out.println("Found smthg to print");
    //lines.print();
//@ TODO convert into Doube array and pass to predict
/*JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

    @Override

    public Iterator<String> call(String x) {

      return Arrays.asList(SPACE.split(x)).iterator();

    }

  });

    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
      new PairFunction<String, String, Integer>() {
        @Override
        public Tuple2<String, Integer> call(String s) {
          return new Tuple2<>(s, 1);
        }
      }).reduceByKey(new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      });

    wordCounts.print();*/
    jssc.start();
    jssc.awaitTermination();
  }

@Override
public boolean processAndPredict() {
	// TODO Auto-generated method stub
	return false;
}

@Override
public boolean report() {
	// TODO Auto-generated method stub
	return false;
}

@Override
public boolean init() {
	// TODO Auto-generated method stub
	return false;
}

@Override
public boolean init(JavaSparkContext jsc) {
	// TODO Auto-generated method stub
	return false;
}

@Override
public boolean processAndPredict(String input) {
	// TODO Auto-generated method stub
	return false;
}
}