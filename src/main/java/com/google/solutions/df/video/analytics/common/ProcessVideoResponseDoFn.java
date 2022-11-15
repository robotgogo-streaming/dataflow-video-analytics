/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.solutions.df.video.analytics.common;

import java.util.Collection;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.videointelligence.v1p3beta1.StreamingAnnotateVideoResponse;
import com.google.solutions.df.video.analytics.bigquery.BQDestination;
import com.google.solutions.df.video.analytics.videointelligence.AnnotateVideoResponseProcessor;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProcessImageResponse {@link AnnotateVideoResponseProcessor} class parses the video response for
 * specific annotation and using image response builder output the table and table row for BigQuery
 */
@AutoValue
abstract public class ProcessVideoResponseDoFn
    extends DoFn<KV<String, StreamingAnnotateVideoResponse>, KV<BQDestination, TableRow>> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ProcessVideoResponseDoFn.class);

  abstract Collection<AnnotateVideoResponseProcessor> processors();

  abstract Counter processedFileCounter();

  public static ProcessVideoResponseDoFn create(
      Collection<AnnotateVideoResponseProcessor> processors) {
    return builder()
        .processors(processors)
        .processedFileCounter(Metrics
            .counter(ProcessVideoResponseDoFn.class, "processedFiles"))
        .build();
  }

  @ProcessElement
  public void processElement(@Element KV<String, StreamingAnnotateVideoResponse> element,
      OutputReceiver<KV<BQDestination, TableRow>> out) {
    String fileURI = element.getKey();
    StreamingAnnotateVideoResponse annotationResponse = element.getValue();

    LOG.debug("Processing annotations for file: {}", fileURI);
    processedFileCounter().inc();

    processors().forEach(processor -> {
      Iterable<KV<BQDestination, TableRow>> processingResult = processor
          .process(fileURI, annotationResponse);
      if (processingResult != null) {
        processingResult.forEach(out::output);
      }
    });
  }

  public static Builder builder() {
    return new AutoValue_ProcessVideoResponseDoFn.Builder();
  }


  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder processors(Collection<AnnotateVideoResponseProcessor> processors);

    public abstract Builder processedFileCounter(Counter processedFileCounter);

    public abstract ProcessVideoResponseDoFn build();
  }
}
