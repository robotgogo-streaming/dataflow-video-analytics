/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.solutions.df.video.analytics.common;

import com.google.api.gax.rpc.BidiStream;
import com.google.auto.value.AutoValue;
import com.google.cloud.videointelligence.v1p3beta1.StreamingAnnotateVideoRequest;
import com.google.cloud.videointelligence.v1p3beta1.StreamingAnnotateVideoResponse;
import com.google.cloud.videointelligence.v1p3beta1.StreamingFeature;
import com.google.cloud.videointelligence.v1p3beta1.StreamingObjectTrackingConfig;
import com.google.cloud.videointelligence.v1p3beta1.StreamingVideoConfig;
import com.google.cloud.videointelligence.v1p3beta1.StreamingVideoIntelligenceServiceClient;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import java.io.IOException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends the given video chunks to the Video Intelligence API and outputs the resulting annotations.
 */
@AutoValue
@SuppressWarnings("serial")
public abstract class AnnotateVideoChunksTransform
    extends PTransform<PCollection<KV<String, ByteString>>, PCollection<Row>> {
  private static final Logger LOG = LoggerFactory.getLogger(AnnotateVideoChunksTransform.class);
  private static final TupleTag<KV<String, StreamingAnnotateVideoResponse>>
      apiResponseSuccessElements = new TupleTag<KV<String, StreamingAnnotateVideoResponse>>() {};
  private static final TupleTag<Row> apiResponseFailedElements = new TupleTag<Row>() {};

  public abstract StreamingFeature features();

  public abstract String errorTopic();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setFeatures(StreamingFeature features);

    public abstract Builder setErrorTopic(String errorTopic);

    public abstract AnnotateVideoChunksTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_AnnotateVideoChunksTransform.Builder();
  }

  @Override
  public PCollection<Row> expand(PCollection<KV<String, ByteString>> input) {
    PCollectionTuple videoApiResults =
        input.apply(
            ParDo.of(new StreamingAnnotate(features()))
                .withOutputTags(
                    apiResponseSuccessElements, TupleTagList.of(apiResponseFailedElements)));

    // Fork out API call failures to a separate branch of the pipeline
    videoApiResults
        .get(apiResponseFailedElements)
        .setRowSchema(Util.errorSchema)
        .apply("ConvertToJson", ToJson.of())
        .apply(
            "ConvertToPubSubMessage",
            ParDo.of(
                new DoFn<String, PubsubMessage>() {

                  @ProcessElement
                  public void processContext(ProcessContext c) {
                    LOG.error("Error {}", c.element());
                    c.output(
                        new PubsubMessage(
                            c.element().getBytes(), ImmutableMap.of("error_type", "api_response")));
                  }
                }))
        .apply("PublishErrorMessage", PubsubIO.writeMessages().to(errorTopic()));

    // Format the annotations returned by the successful API calls
    return videoApiResults
        .get(apiResponseSuccessElements)
        .apply("ProcessResponse", ParDo.of(new FormatAnnotationSchemaDoFn()));
  }

  public static class StreamingAnnotate
      extends DoFn<KV<String, ByteString>, KV<String, StreamingAnnotateVideoResponse>> {

    private final StreamingFeature features;
    private final Counter numberOfRequests =
        Metrics.counter(AnnotateVideoChunksTransform.class, "numberOfRequests");
    private StreamingVideoConfig streamingVideoConfig;
    BidiStream<StreamingAnnotateVideoRequest, StreamingAnnotateVideoResponse> streamCall;

    public StreamingAnnotate(StreamingFeature features) {
      this.features = features;
    }

    // [START loadSnippet_2]
    @Setup
    public void setup() throws IOException {
      StreamingObjectTrackingConfig objectTrackingConfig =
          StreamingObjectTrackingConfig.newBuilder().build();
      streamingVideoConfig =
          StreamingVideoConfig.newBuilder()
              .setFeature(features)
              .setObjectTrackingConfig(objectTrackingConfig)
              .build();
    }

    @ProcessElement
    public void processElement(@Element KV<String, ByteString> element, MultiOutputReceiver out)
        throws IOException {
      String fileName = element.getKey();
      ByteString chunk = element.getValue();
      try (StreamingVideoIntelligenceServiceClient client =
          StreamingVideoIntelligenceServiceClient.create()) {
        streamCall = client.streamingAnnotateVideoCallable().call();
        streamCall.send(
            StreamingAnnotateVideoRequest.newBuilder()
                .setVideoConfig(streamingVideoConfig)
                .build());
        streamCall.send(StreamingAnnotateVideoRequest.newBuilder().setInputContent(chunk).build());
        // [END loadSnippet_2]
        numberOfRequests.inc();
        streamCall.closeSend();
        for (StreamingAnnotateVideoResponse response : streamCall) {
          out.get(apiResponseSuccessElements).output(KV.of(fileName, response));
          if (response.hasError()) {
            Row errorRow =
                Row.withSchema(Util.errorSchema)
                    .addValues(
                        fileName,
                        Util.getCurrentTimeStamp(),
                        response.getError().getCode(),
                        response.getError().getMessage())
                    .build();
            out.get(apiResponseFailedElements).output(errorRow);
          }
        }
      }
    }
  }
}
