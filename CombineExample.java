package com.deloitte.beam.wordCount;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;


public class CombineExample extends CombineFn<Integer, CombineExample.Accum, Double> {
	   public static class Accum {
		     int sum = 0;
		     int count = 0;
		   }
		   public Accum createAccumulator() {
		     return new Accum();
		   }
		   public Accum addInput(Accum accum, Integer input) {
		       accum.sum += input;
		       accum.count++;
		       return accum;
		   }
		   public Accum mergeAccumulators(Iterable<Accum> accums) {
		     Accum merged = createAccumulator();
		     for (Accum accum : accums) {
		       merged.sum += accum.sum;
		       merged.count += accum.count;
		     }
		     return merged;
		   }
		   public Double extractOutput(Accum accum) {
		     return ((double) accum.sum) / accum.count;
		   }
		   
		   public static void main(String args[]) {
			   
			   Pipeline p = Pipeline.create(
						  PipelineOptionsFactory.fromArgs(args).withValidation().create());
			   
			   PCollection<Integer> pc = p.apply(Create.of(3, 4, 5));
			   
				/*
				 * CoderRegistry cr = p.getCoderRegistry(); cr.registerCoder(Double.class);
				 */

			   PCollection<Double> sum = pc.apply(
					   Combine.globally(new CombineExample()));
			   System.out.println(sum);
			   p.run().waitUntilFinish();
		   }
		 }

