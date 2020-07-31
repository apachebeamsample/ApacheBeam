package com.deloitte.beam.wordCount;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class MetricsExample {

	@SuppressWarnings("unchecked")
	public static void main(String args[]) {
		Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());
		PCollection<String> abc = p.apply(TextIO.read().from("/src/main/resources/samplefile2.txt"))
				.apply(ParDo.of(new MyMetricsDoFn()));

		org.apache.beam.sdk.PipelineResult result = p.run();

		MetricQueryResults metrics = (MetricQueryResults) result.metrics().queryMetrics(
				MetricsFilter.builder().addNameFilter(MetricNameFilter.named("namespace", "counter1")).build());

		for (MetricResult<Long> counter : metrics.getCounters()) {
			System.out.println(counter.getName() + ":" + counter.getAttempted());
		}

	}

}
