package com.deloitte.beam.wordCount;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class AdditionalOutput {

	public static void main(String[] args) {

		// public static void main(String[] args) {

		PipelineOptions options = PipelineOptionsFactory.create();

		Pipeline p = Pipeline.create(options);
		PCollection<String> words = p.apply(TextIO.read().from("/src/main/resources/samplefile2.txt"));

		final int wordLengthCutOff = 3;

		final TupleTag<String> wordsBelowCutOffTag = new TupleTag<String>() {
		};
		final TupleTag<Integer> wordLengthsAboveCutOffTag = new TupleTag<Integer>() {
		};
		final TupleTag<String> markedWordsTag = new TupleTag<String>() {
		};

		PCollectionTuple results = words.apply(ParDo.of(new DoFn<String, String>() {

			@ProcessElement
			public void processElement(ProcessContext c) {
				String word = c.element();
				if (word.length() <= wordLengthCutOff) {
					// Emit short word to the main output.
					// In this example, it is the output with tag wordsBelowCutOffTag.
					c.output(wordsBelowCutOffTag, word);
				} else {
					// Emit long word length to the output with tag wordLengthsAboveCutOffTag.
					c.output(wordLengthsAboveCutOffTag, word.length());
				}
				if (word.startsWith("Apache")) {
					c.output(markedWordsTag, word);
				}
			}
		}).withOutputTags(wordsBelowCutOffTag, TupleTagList.of(wordLengthsAboveCutOffTag).and(markedWordsTag)));

		p.run().waitUntilFinish();

	}
}
