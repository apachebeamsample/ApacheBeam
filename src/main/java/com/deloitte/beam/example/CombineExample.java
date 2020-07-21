package com.deloitte.beam.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class CombineExample {

	public static void main(String args[]) {

		Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());
		PCollection<Integer> pc = p.apply(Create.of(3, 4, 5));
		PCollection<Integer> sum = pc.apply(Combine.globally(new SumIntegers()));
		// sum.apply(TextIO.write().to(""));
		sum.apply(ParDo.of(new DoFn<Integer, Void>() {
			@ProcessElement
			public void processElement(ProcessContext c) {
				System.out.println(c.element());
			}
		}));
		p.run().waitUntilFinish();
	}

}
