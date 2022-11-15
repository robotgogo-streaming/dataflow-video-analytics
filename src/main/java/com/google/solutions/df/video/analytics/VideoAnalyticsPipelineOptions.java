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
package com.google.solutions.df.video.analytics;

import java.util.List;
import java.util.Set;

import com.google.cloud.videointelligence.v1p3beta1.StreamingFeature;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface VideoAnalyticsPipelineOptions extends PipelineOptions {

  @Description("Pub/Sub subscription ID to receive input Cloud Storage notifications from")
  String getInputNotificationSubscription();

  void setInputNotificationSubscription(String value);

  @Description("Pub/Sub topic ID to publish the results to")
  String getOutputTopic();

  void setOutputTopic(String value);

  @Description("Pub/Sub topic ID to publish the error message")
  String getErrorTopic();

  void setErrorTopic(String value);

  @Description("Streaming video feature")
  StreamingFeature getFeature();

  void setFeature(StreamingFeature value);

  @Description("Window time interval (in seconds) for outputing results")
  @Default.Integer(1)
  Integer getWindowInterval();

  void setWindowInterval(Integer value);

  @Description(
      "Comma-separated list of entity labels. Object tracking annotations must contain at least one of those labels to be considered significant and be published the output Pub/Sub topic")
  List<String> getEntities();

  void setEntities(List<String> value);

  @Description(
      "Minimum confidence level that the object tracking annotations must meet to be considered significant and be published the output Pub/Sub topic")
  @Default.Double(0.5)
  Double getConfidenceThreshold();

  void setConfidenceThreshold(Double value);

  @Description("Simulate annotations")
  @Default.Boolean(false)
  boolean isSimulate();

  void setSimulate(boolean value);

  @Description("Key range")
  @Default.Integer(1)
  Integer getKeyRange();

  void setKeyRange(Integer value);

  @Description("Video annotation request batch size")
  @Default.Integer(1)
  Integer getBatchSize();

  void setBatchSize(Integer value);

  @Description("Project id to be used for Video Intelligence API requests and BigQuery dataset")
  @Validation.Required
  String getProjectId();

  void setProjectId(String value);

  @Description("BigQuery dataset")
  @Validation.Required
  String getDatasetName();

  void setDatasetName(String value);

  @Description("Table name for object tracking annotations")
  @Validation.Required
  String getObjectTrackingAnnotationsTable();

  void setObjectTrackingAnnotationsTable(String value);

  @Description("Table name for label annotations")
  @Validation.Required
  String getLabelAnnotationsTable();

  void setLabelAnnotationsTable(String value);


  @Description("GCS metadata values to store in BigQuery")
  @Validation.Required
  Set<String> getMetadataKeys();

  void setMetadataKeys(Set<String> value);
}
