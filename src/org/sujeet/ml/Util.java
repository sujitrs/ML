package org.sujeet.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

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
	        	  System.out.print("sarray["+i+"]="+sarray[i]+",");
	        	  
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
		            System.out.print("Converted value="+values[i]);	
	            }
	            	
	          } System.out.println("");
	          return new LabeledPoint(label, Vectors.dense(values));
	        }
	      }
	    );
	    return parsedData;
	   
}
}
