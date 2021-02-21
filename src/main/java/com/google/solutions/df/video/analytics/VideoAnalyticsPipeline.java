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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.videointelligence.v1p3beta1.StreamingAnnotateVideoResponse;
import com.google.common.collect.ImmutableSet;
import com.google.solutions.df.video.analytics.bigquery.BQDestination;
import com.google.solutions.df.video.analytics.bigquery.BQDynamicDestinations;
import com.google.solutions.df.video.analytics.bigquery.BigQueryDynamicWriteTransform;
import com.google.solutions.df.video.analytics.bigquery.TableDetails;
import com.google.solutions.df.video.analytics.common.*;
import com.google.solutions.df.video.analytics.videointelligence.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class VideoAnalyticsPipeline {

  public static final Counter totalFiles = Metrics
          .counter(VideoAnalyticsPipeline.class, "totalFiles");
  public static final Counter rejectedFiles = Metrics
          .counter(VideoAnalyticsPipeline.class, "rejectedFiles");
  public static final Counter numberOfRequests = Metrics
          .counter(VideoAnalyticsPipeline.class, "numberOfRequests");
  public static final Counter numberOfQuotaExceededRequests = Metrics
          .counter(VideoAnalyticsPipeline.class, "numberOfQuotaExceededRequests");

  private static final Set<String> SUPPORTED_CONTENT_TYPES = ImmutableSet.of(
          "video/mov", "video/mpeg4", "video/mp4", "video/avi"
  );

  public static void main(String[] args) {
    VideoAnalyticsPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(VideoAnalyticsPipelineOptions.class);
    run(options);
  }

  private static void run(VideoAnalyticsPipelineOptions options) {
    Pipeline p = Pipeline.create(options);

    Map<String, AnnotateVideoResponseProcessor> processors = configureProcessors(options);

//    // Ingest and validate input GCS notifications
//    PCollection<KV<String, ByteString>> videoFilesWithContext =
//        p.apply(
//            "FilterInputNotifications",
//            FilterInputNotificationsTransform.newBuilder()
//                .setSubscriptionId(options.getInputNotificationSubscription())
//                .build());

    PCollection<String> fileUris = convertPubSubNotificationsToGCSURIs(p, options);

//    PCollection<Iterable<String>> batchedURIs = fileUris
//            .apply("Batch files",
//                    BatchRequestsTransform.create(options.getBatchSize(), options.getKeyRange()));

    PCollection<KV<String, StreamingAnnotateVideoResponse>> annotatedFiles =
            options.isSimulate() ?
                    fileUris.apply("Simulate Annotation",
                            ParDo.of(new AnnotateSimulatorDoFn())) :
                    fileUris.apply(
                            "Annotate files",
                            AnnotateVideosTransform.newBuilder().setFeatures(options.getFeature()).build());

    // Call the Video ML API to annotate the ingested video clips
    PCollection<KV<BQDestination, TableRow>> annotationOutcome =
            annotatedFiles.apply(
                    "Process Annotations",
                    ParDo.of(ProcessVideoResponseDoFn.create(ImmutableSet.copyOf(processors.values()))));

    annotationOutcome.apply("Write To BigQuery", new BigQueryDynamicWriteTransform(
                    BQDynamicDestinations.builder()
                            .projectId(options.getProjectId())
                            .datasetId(options.getDatasetName())
                            .metadataKeys(options.getMetadataKeys())
                            .tableNameToTableDetailsMap(
                                    tableNameToTableDetailsMap(processors)).build())
    );

//    PCollection<Row> annotationResult =
//        videoFilesWithContext
//            .apply(
//                "AnnotateVideoChunks",
//                AnnotateVideoChunksTransform.newBuilder()
//                    .setFeatures(options.getFeatures())
//                    .setErrorTopic(options.getErrorTopic())
//                    .build())
//            .setRowSchema(Util.videoMlCustomOutputSchema)
//            .apply(
//                "FixedWindow",
//                Window.<Row>into(
//                        FixedWindows.of(Duration.standardSeconds(options.getWindowInterval())))
//                    .triggering(AfterWatermark.pastEndOfWindow())
//                    .discardingFiredPanes()
//                    .withAllowedLateness(Duration.ZERO))
//            .apply("GroupAnnotationsResponse", new GroupByAnnotateResponseTransform())
//            .setRowSchema(Util.videoMlCustomOutputListSchema);
//
//    // Filter annotations by relevant entities and confidence, then write to Pub/Sub output topic
//    annotationResult
//        .apply(
//            "FilterRelevantAnnotations",
//            FilterRelevantAnnotationsTransform.newBuilder()
//                .setEntityList(options.getEntities())
//                .setConfidenceThreshold(options.getConfidenceThreshold())
//                .build())
//        .setRowSchema(Util.videoMlCustomOutputListSchema)
//        .apply(
//            "WriteRelevantAnnotationsToPubSub",
//            WriteRelevantAnnotationsToPubSubTransform.newBuilder()
//                .setTopicId(options.getOutputTopic())
//                .build());
//
//    // Stream insert all annotations to BigQuery
//    annotationResult.apply(
//        "WriteAllAnnotationsToBigQuery",
//        WriteAllAnnotationsToBigQueryTransform.newBuilder()
//            .setTableReference(options.getTableReference())
//            .setMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
//            .build());

    p.run();
  }


  /**
   * Reads PubSub messages from the subscription provided by {@link VideoAnalyticsPipelineOptions#getInputNotificationSubscription()}.
   *
   * The messages are expected to confirm to the GCS notification message format defined in
   * https://cloud.google.com/storage/docs/pubsub-notifications
   *
   * Notifications are filtered to have one of the supported content types: {@link
   * VideoAnalyticsPipeline#SUPPORTED_CONTENT_TYPES}.
   *
   * @return PCollection of GCS URIs
   */
  static PCollection<String> convertPubSubNotificationsToGCSURIs(
          Pipeline p, VideoAnalyticsPipelineOptions options) {
    PCollection<String> fileUris;
    PCollection<PubsubMessage> pubSubNotifications = p.begin().apply("Read PubSub",
            PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInputNotificationSubscription()));
    fileUris = pubSubNotifications
            .apply("PubSub to GCS URIs",
                    ParDo.of(PubSubNotificationToGCSUriDoFn.create(SUPPORTED_CONTENT_TYPES)))
            .apply(
                    "Fixed Window",
                    Window.<String>into(
                            FixedWindows.of(Duration.standardSeconds(options.getWindowInterval())))
                            .triggering(AfterWatermark.pastEndOfWindow())
                            .discardingFiredPanes()
                            .withAllowedLateness(Duration.standardMinutes(15)));
    return fileUris;
  }

  /**
   * Create a map of the table details. Each processor will produce <code>TableRow</code>s destined
   * to a different table. Each processor will provide the details about that table.
   *
   * @return map of table details keyed by table name
   */
  static Map<String, TableDetails> tableNameToTableDetailsMap(
          Map<String, AnnotateVideoResponseProcessor> processors) {
    Map<String, TableDetails> tableNameToTableDetailsMap = new HashMap<>();
    processors.forEach(
            (tableName, processor) -> tableNameToTableDetailsMap
                    .put(tableName, processor.destinationTableDetails()));
    return tableNameToTableDetailsMap;
  }

  /**
   * Creates a map of well-known {@link AnnotateVideoResponseProcessor}s.
   *
   * If additional processors are needed they should be configured in this method.
   */
  private static Map<String, AnnotateVideoResponseProcessor> configureProcessors(
          VideoAnalyticsPipelineOptions options) {
    Map<String, AnnotateVideoResponseProcessor> result = new HashMap<>();

    String tableName = options.getObjectTrackingAnnotationsTable();
    result.put(tableName, new ObjectTrackingAnnotationProcessor(tableName));

    tableName = options.getLabelAnnotationsTable();
    result.put(tableName, new LabelAnnotationProcessor(tableName, options.getMetadataKeys()));

    return result;
  }

}
