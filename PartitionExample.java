package com.deloitte.beam.wordCount;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.commons.codec.net.PercentCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionExample {

	public static void main(String[] args) throws Exception {

		Logger logger = LoggerFactory.getLogger(PartitionExample.class);

		PipelineOptions options = PipelineOptionsFactory.create();

		Pipeline p = Pipeline.create(options);

		PCollection<String> textData = p.apply(TextIO.read()
				.from("/src/main/resources/Partition.txt"));
	
		PCollectionList<String> studentsByPercentile =
				textData.apply(Partition.of(3, new PartitionFn<String>() {

					@Override
					public int partitionFor(String elem, int numPartitions) {
						String[] result= elem.split(",", 4);
						System.out.println(result[0]);
						System.out.println(result[1]);
						int finalresult = Integer.parseInt(result[1]);
						int partitioned = finalresult/10000;
						//return result;
						return partitioned;
					}}));

		//System.out.println(studentsByPercentile);
	//	for (int i = 0; i < 4; i++) {
	
	  PCollection<String> partition = studentsByPercentile.get(1);
	  partition.apply(TextIO.write().to("./src/main/resources/partitionoutput.txt")
	  );
	 
			 
			// }
		
		//PCollection<String> merged = collections.apply(Flatten.<String>pCollections());
		/*
		 * PCollection<String> finalresult = studentsByPercentile.get(2);
		 * 
		 * finalresult.apply(TextIO.write().to(
		 * "./src/main/resources/partitionoutput.txt"));
		 */
		/*
		 * textData.apply(TextIO.write().to("./src/main/resources/flattenoutput.txt").
		 * withNumShards(1)); System.out.println(studentsByPercentile.get(3));
		 * 
		 */		
		// Pipeline
		p.run().waitUntilFinish();

		System.exit(0);
	}
}
