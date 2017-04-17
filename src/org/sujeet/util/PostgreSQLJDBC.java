package org.sujeet.util;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sujeet.ml.analyse.Analysis;

public class PostgreSQLJDBC {
	static final Logger logger = LogManager.getLogger(PostgreSQLJDBC.class.getName());
	static public Integer id;
	static Connection c = null;
	public static void init() {
	      
	      try {
	         Class.forName("org.postgresql.Driver");
	         c = DriverManager
	            .getConnection("jdbc:postgresql://localhost:5432/postgres",
	            "postgres", "nsdl@123");
	      } catch (Exception e) {
	         e.printStackTrace();
	         logger.error(e.getClass().getName()+": "+e.getMessage());
	         System.exit(0);
	      }
	      logger.debug("Opened database successfully");
	      registerRun(c);
	   }	

	public static void registerRun(Connection c){

	      Statement stmt = null;
	      try {
	    	  c.setAutoCommit(false);
	         stmt = c.createStatement();
	         String sql = "INSERT INTO run_summary(run) VALUES (current_timestamp);";
	         stmt.executeUpdate(sql);
	         stmt.close();
	         stmt = c.createStatement();
	         ResultSet rs = stmt.executeQuery( "SELECT max(id) id FROM run_summary;" );
	         while ( rs.next() ) {
	            id = rs.getInt("id");
	            logger.debug( "ID = " + id );
	         }
	         rs.close();
	         stmt.close();
	         c.commit();
	      } catch (Exception e) {
	         logger.error( e.getClass().getName()+": "+ e.getMessage() );
	         try {
				c.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				System.exit(0);
			}
	
	      }
	      
	   }
	
	public static void saveRunDetails(Integer id2, int tp, int tn, int fp, int fn, double accuracy, double precision,
			double recall, double fMeasure, int algoID, String model, double d, double f, double g, double h, double i, double j, double k, double l) {
	         String sql = "INSERT INTO run_details  VALUES ("+id2+", "+accuracy+", "+precision+", "+recall+", "+fMeasure+","+algoID+",'"+model+"',"+d+","+f+","+g+","+h+","+i+","+j+","+k+","+l+")";
	         insert(sql);
	
	      }

	public static void saveDatasetAndResults(String dataset,double RFprediction,double DTprediction,double LRprediction,double finalPrediction) {
		
	         String sql = "INSERT INTO predictions  VALUES ("+dataset+","+RFprediction+","+DTprediction+","+LRprediction+","+finalPrediction+")";
	         System.out.println("Prediction inserted with"+sql );
	         insert(sql);
	         System.out.println("Prediction inserted with"+sql );
	      }
	
	
	
	public static void insert(String sql) {
		Statement stmt = null;
	      try {
	    	  c.setAutoCommit(false);
	         stmt = c.createStatement();
	         stmt.executeUpdate(sql);
	         stmt.close();
	         c.commit();
	      } catch (Exception e) {
	         logger.error( e.getClass().getName()+": "+ e.getMessage() );
	         try {
				c.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
				System.exit(0);
			}
	
	      }
	
	}
	

}