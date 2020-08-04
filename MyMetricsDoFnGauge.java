package com.deloitte.beam.wordCount;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

public class MyMetricsDoFnGauge extends DoFn<String,String> {

	 private final Gauge gauge = Metrics.gauge( "namespace", "gauge1");
	 @ProcessElement
     public void processElement(ProcessContext context) {
		 
		 Integer element = Integer.parseInt(context.element());
		 gauge.set(element);
		
		 
   
     }
	 
	  }
	
