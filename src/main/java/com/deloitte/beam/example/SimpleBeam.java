package com.deloitte.beam.example;

import java.io.File;
import java.net.URL;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleBeam {

	public static void main(String[] args) throws Exception {

		Logger logger = LoggerFactory.getLogger(SimpleBeam.class);

		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline pipeline = Pipeline.create(options);
		PCollection<String> textData = pipeline.apply(TextIO.read()
				.from("/Users/marcrisney/Projects/Misc/apache-beam-example/src/main/resources/sample.txt"));

		textData.apply(MapElements.into(TypeDescriptors.strings()).via(s -> s + "/")).apply(TextIO.write()
				.to("/Users/marcrisney/Projects/Misc/apache-beam-example/src/main/resources/wordcounts.txt"));
		// Pipeline
		pipeline.run().waitUntilFinish();

		System.exit(0);
	}
}
