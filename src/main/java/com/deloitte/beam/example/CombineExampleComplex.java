package com.deloitte.beam.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class CombineExampleComplex extends CombineFn<Integer, CombineExampleComplex.Accum, Double> {
	
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

		Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

		PCollection<Integer> pc = p.apply(Create.of(1, 3, 5, 7, 9));

		PCollection<Integer> sum = pc.apply(Combine.globally(new SumIntegers()));

		sum.apply(ParDo.of(new DoFn<Integer, Void>() {
			@ProcessElement
			public void processElement(ProcessContext c) {
				System.out.println(c.element());
			}
		}));

		p.run().waitUntilFinish();
	}

}