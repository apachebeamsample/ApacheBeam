package com.deloitte.beam.RestDemo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RestDemoMain {

	private static final Logger LOG = LoggerFactory.getLogger(RestDemoMain.class);
	public static void main(String[] args) {

		Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());
		
		PCollection<String> users=pipeline.apply(TextIO.read().from("/src/resources/sample.json"));
		PCollection<UserDetails> userList=users.apply(ParseJsons.of(UserDetails.class)).setCoder(SerializableCoder.of(UserDetails.class));
		userList.apply(MapElements.into(TypeDescriptors.strings()).via(u -> u.getUserId()))
				.apply(TextIO.write().to("./src/output/user.txt").withNumShards(1));
		pipeline.run().waitUntilFinish();
	}

}
