package com.deloitte.beam.wordCount;

import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.values.PCollection;

public class CsvFileFullReader {
	
	/*
	 * // The Filter class static class RemoveHeader extends DoFn<String, String> {
	 * String headerFilter;
	 * 
	 * public RemoveHeader(String headerFilter) { this.headerFilter = headerFilter;
	 * }
	 * 
	 * 
	 * 
	 * @ProcessElement public void processElement(ProcessContext c) { String row =
	 * (String) c.element(); // Filter out elements that match the header if
	 * (!row.equals(this.headerFilter)) { c.output(row); } } }
	 */
		  
		  
		  
		// The Filter class
			  static class ColumnSelector extends DoFn<String, String> {
			    
			
			    @ProcessElement
			    public void processElement(ProcessContext c) {
			      String row = (String) c.element();
			      String[] cols = row.split(",");
			      
			      // Filter out elements that match the header
			     	        c.output(cols[1]+","+cols[7]+","+cols[8]);
			      
			    }
			  }
			  
			// The Filter class
			  static class DecimalConverter extends DoFn<String, String> {
			    
			
			    @ProcessElement
			   // public void processElement(ProcessContext c) {
			     
			    	 public void processElement(ProcessContext c) {
					      String row = (String) c.element();
					      String[] cols = row.split(",");
							/*
							 * for(int i=0;i<cols.length;i++) {
							 * 
							 * if(cols[i].contains(".")) {
							 * 
							 * //c.output(finalString); //return finalString; } }
							 */
					      String colsString0 = "";
					      String colsString1 = "";
					      String colsString2 = "";
					      
					      	if(cols[0].contains(".")) {
					      		 colsString0 = getDecimal(cols[0].toString());
					      	}else {
					      		colsString0 = cols[0].toString();
					      	}
					      	if(cols[1].contains(".")) {
					      		 colsString1 = getDecimal(cols[1].toString());
					      	}else {
					      		colsString1 = cols[1].toString();
					      	}
					      	if(cols[2].contains(".")) {
					      		 colsString2 = getDecimal(cols[2].toString());
					      	}else {
					      		colsString2 = cols[2].toString();
					      	}
					      	c.output(colsString0+","+colsString1+","+colsString2);
					    }
			    }
			  
		
			  
			  
			  static String getDecimal(String cols) {
				  String[] str = cols.split("\\.");
		    		 String finalString = str[0];
		    		 return finalString;
				
				  
			  }
		public static void main(String[] args) {

			 Pipeline p = Pipeline.create(
				        PipelineOptionsFactory.fromArgs(args).withValidation().create());
			 
			 /*PCollection<KV<String,Long>> branchDeatils=pipe.apply(TextIO.read().from("/src/main/resources/StudentDetail.csv"))
					 									.apply("Remove header row",
					 									        Filter.by((String row) -> !((row.startsWith("Sl No") || row.startsWith("\"sl no\"")
					 									               || row.startsWith("'sl'")))))*/
			 
			 PCollection<String> vals = p.apply(TextIO.read().from("/src/main/resources/FL_insurance_sample.csv"));

			    String header = "policyID,statecode,county,eq_site_limit,hu_site_limit,fl_site_limit,fr_site_limit,tiv_2011,tiv_2012,eq_site_deductible,hu_site_deductible,fl_site_deductible,fr_site_deductible,point_latitude,point_longitude,line,construction,point_granularity";

				vals/* .apply(ParDo.of(new RemoveHeader(header))) */
						.apply(ParDo.of(new ColumnSelector()))
						 .apply(ParDo.of(new DecimalConverter())) .apply(TextIO.write().to("./src/main/resources/FL_insurance_sample").withSuffix(".csv").withNumShards(1));

			    p.run().waitUntilFinish();
			 
		}

}
