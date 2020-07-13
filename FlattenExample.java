package com.deloitte.beam.wordCount;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlattenExample {

	public static void main(String[] args) throws Exception {

		Logger logger = LoggerFactory.getLogger(FlattenExample.class);

		PipelineOptions options = PipelineOptionsFactory.create();

		Pipeline p = Pipeline.create(options);

		PCollection<String> textData = p.apply(TextIO.read()
				.from("/src/main/resources/flatten1.txt"));
		PCollection<String> textData1 = p.apply(TextIO.read()
				.from("/src/main/resources/flatten2.txt"));
		PCollection<String> textData2 = p.apply(TextIO.read()
				.from("/src/main/resources/flatten3.txt"));

		PCollectionList<String> collections = PCollectionList.of(textData).and(textData1).and(textData2);

		PCollection<String> merged = collections.apply(Flatten.<String>pCollections());
merged.apply(TextIO.write().to("./src/main/resources/flattenoutput.txt").withNumShards(1));
		
		// Pipeline
		p.run().waitUntilFinish();

		System.exit(0);
	}
}
