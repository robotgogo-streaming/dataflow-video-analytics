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
package com.google.solutions.df.video.analytics.videointelligence;

import java.io.IOException;

import com.google.api.gax.rpc.BidiStream;
import com.google.auto.value.AutoValue;
import com.google.cloud.videointelligence.v1p3beta1.*;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

/**
 * Sends the given video files to the Video Intelligence API and outputs the resulting annotations.
 */
@AutoValue
@SuppressWarnings("serial")
public abstract class AnnotateVideosTransform
    extends PTransform<PCollection<String>, PCollection<KV<String, StreamingAnnotateVideoResponse>>> {

  public abstract StreamingFeature features();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setFeatures(StreamingFeature features);

    public abstract AnnotateVideosTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_AnnotateVideosTransform.Builder();
  }

  @Override
  public PCollection<KV<String, StreamingAnnotateVideoResponse>> expand(PCollection<String> input) {
    return
        input
            .apply("FindFile", FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW))
            .apply(FileIO.readMatches())
            .apply("ReadFile", ParDo.of(new ReadFile()))
            .apply(ParDo.of(new StreamingAnnotate(features())));
  }


  public static class ReadFile extends DoFn<FileIO.ReadableFile, KV<String, ByteString>> {
    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      FileIO.ReadableFile file = c.element();
      String fileName = file.getMetadata().resourceId().toString();
      c.output(KV.of(fileName, ByteString.copyFrom(file.readFullyAsBytes())));
    }
  }


  public static class StreamingAnnotate
      extends DoFn<KV<String, ByteString>, KV<String, StreamingAnnotateVideoResponse>> {

    private final StreamingFeature feature;
    private final Counter numberOfRequests =
        Metrics.counter(AnnotateVideosTransform.class, "numberOfRequests");
    private StreamingVideoConfig streamingVideoConfig;
    BidiStream<StreamingAnnotateVideoRequest, StreamingAnnotateVideoResponse> streamCall;

    public StreamingAnnotate(StreamingFeature feature) {
      this.feature = feature;
    }

    @Setup
    public void setup() throws IOException {
      StreamingObjectTrackingConfig objectTrackingConfig =
          StreamingObjectTrackingConfig.newBuilder().build();
      StreamingLabelDetectionConfig labelConfig =
              StreamingLabelDetectionConfig.newBuilder().build();
      streamingVideoConfig =
          StreamingVideoConfig.newBuilder()
              .setFeature(feature)
              .setObjectTrackingConfig(objectTrackingConfig)
              .setLabelDetectionConfig(labelConfig)
              .build();
    }

    @ProcessElement
    public void processElement(ProcessContext c)
        throws IOException {
      String fileName = c.element().getKey();
      ByteString chunk = c.element().getValue();
      try (StreamingVideoIntelligenceServiceClient client =
          StreamingVideoIntelligenceServiceClient.create()) {
        streamCall = client.streamingAnnotateVideoCallable().call();
        streamCall.send(
            StreamingAnnotateVideoRequest.newBuilder()
                .setVideoConfig(streamingVideoConfig)
                .build());
        streamCall.send(StreamingAnnotateVideoRequest.newBuilder().setInputContent(chunk).build());
        numberOfRequests.inc();
        streamCall.closeSend();
        for (StreamingAnnotateVideoResponse response : streamCall) {
          c.output(KV.of(fileName, response));
        }
      }
    }
  }
}
