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
package com.google.solutions.df.video.analytics.videointelligence;

import java.util.Random;


import com.google.cloud.videointelligence.v1p3beta1.StreamingAnnotateVideoResponse;
import com.google.solutions.df.video.analytics.VideoAnalyticsPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * Annotation simulation class to test batching logic without incurring API costs.
 *
 * It simulates the delay of calling the API and produces a single annotation.
 */
public class AnnotateSimulatorDoFn extends
    DoFn<String, KV<String, StreamingAnnotateVideoResponse>> {
  private static final long serialVersionUID = 1L;

  @ProcessElement
  public void processElement(@Element String fileUri,
      OutputReceiver<KV<String, StreamingAnnotateVideoResponse>> out) {
    VideoAnalyticsPipeline.numberOfRequests.inc();
    try {
      Thread.sleep(500 + (new Random().nextInt(1000)));
    } catch (InterruptedException e) {
      // Do nothing
    }


    StreamingAnnotateVideoResponse.Builder responseBuilder = StreamingAnnotateVideoResponse.newBuilder();
    // TODO: add fake data
    StreamingAnnotateVideoResponse response = responseBuilder.build();
    out.output(KV.of(fileUri, response));
  }
}
