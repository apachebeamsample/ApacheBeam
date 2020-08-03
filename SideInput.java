package com.deloitte.beam.wordCount;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

public class SideInput {

	public static void main(String[] args) {

		PipelineOptions options = PipelineOptionsFactory.create();

		Pipeline p = Pipeline.create(options);
		PCollection<String> words = p.apply(TextIO.read().from("/src/main/resources/samplefile2.txt"));
		 PCollection<Integer> wordLengths = p.apply(Create.of(6));

		final PCollectionView<Integer> maxWordLengthCutOffView =
			     wordLengths.apply(View.asSingleton());
			    
		
		PCollection<String> results = words.apply(ParDo.of(new DoFn<String, String>() {

			@ProcessElement
			public void processElement(@Element String word, OutputReceiver<String> out, ProcessContext c) {
				
				int lengthCutOff = c.sideInput(maxWordLengthCutOffView);
				if (word.length() >= lengthCutOff) {
		              out.output(word);
		              System.out.println(word);
		            }
				
			}
		}).withSideInputs(maxWordLengthCutOffView));

		p.run().waitUntilFinish();

	}
}
