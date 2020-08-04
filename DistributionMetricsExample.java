package com.deloitte.beam.wordCount;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class DistributionMetricsExample {

	@SuppressWarnings("unchecked")
	public static void main(String args[]) {
		Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());
		PCollection<String> abc = p.apply(TextIO.read().from("/src/main/resources/GaugeMetrics.txt"))
				.apply(ParDo.of(new MyMetricsDistributionDoFn()));

		org.apache.beam.sdk.PipelineResult result = p.run();

		MetricQueryResults metrics = (MetricQueryResults) result.metrics().queryMetrics(
				MetricsFilter.builder().addNameFilter(MetricNameFilter.named("namespace", "distribution1")).build());

		for (MetricResult<DistributionResult> distribution : metrics.getDistributions()) {
			System.out.println(distribution.getName() + ":" + distribution.getAttempted());
		}

	}

}
