package com.deloitte.beam.wordCount;

import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

public class MyMetricsDistributionDoFn extends DoFn<String,String> {

	 private final Distribution distribution = Metrics.distribution( "namespace", "distribution1");

	 @ProcessElement
     public void processElement(ProcessContext context) {
		 Integer element = Integer.parseInt(context.element());
		 distribution.update(element);
	
		 
     }
	 
	  }
	
