package com.deloitte.beam.wordCount;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

public class MyMetricsDoFn extends DoFn<String,String> {

	 private final Counter counter = Metrics.counter( "namespace", "counter1");

	 @ProcessElement
     public void processElement(ProcessContext context) {
		 
		  String element =  context.element();
      String [] split= element.split(" ");
      counter.inc();
      counter.inc();
      counter.inc();
      context.output(split[3]);
      System.out.println(split[3]);
   
     }
	 
	  }
	
