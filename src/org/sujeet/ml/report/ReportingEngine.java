package org.sujeet.ml.report;

public interface ReportingEngine{
	public void init();
	public boolean reportAnomaly(String i);
	public boolean reportNormal(String i);	
}