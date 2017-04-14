package org.sujeet.ml.report;
import java.io.Serializable;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sujeet.ml.analyse.Analysis;
public class ReportToKafka implements ReportingEngine,Serializable {
	
	
	
	
	static final Logger logger = LogManager.getLogger(ReportToKafka.class.getName());
	
	static Producer<String, String> producer;
	final static String NORMAL_TOPIC="normal";
	final static String ANOMALY_TOPIC="anomaly";
	
	@Override
	public void init() {
		// TODO Auto-generated method stub
		logger.debug("Start");
		Properties props = new Properties();
		 props.put("bootstrap.servers", "localhost:9092");
		 props.put("acks", "all");
		 props.put("retries", 0);
		 props.put("batch.size", 16384);
		 props.put("linger.ms", 1);
		 props.put("buffer.memory", 33554432);
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		 producer = new KafkaProducer<>(props);

		 logger.debug("End");
	}

	@Override
	public boolean reportAnomaly(String i) {
		// TODO Auto-generated method stub
		logger.debug("Start");
		try{
		producer.send(new ProducerRecord<String, String>(ANOMALY_TOPIC, i, i));
		}catch(Exception ex){
			logger.error(ex.getMessage());
			return false;
		}
		logger.debug("End");
		return true;
	}

	@Override
	public boolean reportNormal(String i) {
		// TODO Auto-generated method stub
				logger.debug("Start");
				try{
				producer.send(new ProducerRecord<String, String>(NORMAL_TOPIC, i, i));
				}catch(Exception ex){
					logger.error(ex.getMessage());
					return false;
				}
				logger.debug("End");
				return true;
	}

}
