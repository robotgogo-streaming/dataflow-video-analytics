package com.google.solutions.df.video.analytics.videointelligence;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.api.services.bigquery.model.*;
import com.google.cloud.videointelligence.v1p3beta1.*;
import com.google.common.collect.ImmutableList;
import com.google.solutions.df.video.analytics.bigquery.BQDestination;
import com.google.solutions.df.video.analytics.bigquery.TableDetails;
import com.google.solutions.df.video.analytics.bigquery.TableSchemaProducer;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.values.KV;
import com.google.solutions.df.video.analytics.videointelligence.Constants.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectTrackingAnnotationProcessor implements AnnotateVideoResponseProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectTrackingAnnotationProcessor.class);
    private final BQDestination destination;
    public final static Counter counter =
            Metrics.counter(AnnotateVideoResponseProcessor.class, "numberOfObjectTrackingAnnotations");

    /**
     * Creates a processor and specifies the table id to persist to.
     */
    public ObjectTrackingAnnotationProcessor(String tableId) {
        destination = new BQDestination(tableId);
    }

    private static class SchemaProducer implements TableSchemaProducer {

        private static final long serialVersionUID = 1L;

        @Override
        public TableSchema getTableSchema() {
            return new TableSchema().setFields(
                    ImmutableList.of(
                            new TableFieldSchema()
                                    .setName(Field.GCS_URI_FIELD)
                                    .setType("STRING")
                                    .setMode("REQUIRED"),
                            new TableFieldSchema()
                                    .setName(Field.TIMESTAMP_FIELD)
                                    .setType("TIMESTAMP")
                                    .setMode("REQUIRED"),
                            new TableFieldSchema()
                                    .setName(Field.ENTITY)
                                    .setType("STRING")
                                    .setMode("REQUIRED"),
                            new TableFieldSchema()
                                    .setName(Field.CONFIDENCE).setType("FLOAT"),
                            new TableFieldSchema()
                                    .setName(Field.FRAMES).setType("RECORD")
                                    .setMode("REPEATED")
                                    .setFields(ImmutableList.of(
                                            new TableFieldSchema()
                                                    .setName(Field.TIME_OFFSET)
                                                    .setType("STRING")
                                                    .setMode("REQUIRED"),
                                            new TableFieldSchema()
                                                    .setName(Field.LEFT)
                                                    .setType("FLOAT")
                                                    .setMode("REQUIRED"),
                                            new TableFieldSchema()
                                                    .setName(Field.TOP)
                                                    .setType("FLOAT")
                                                    .setMode("REQUIRED"),
                                            new TableFieldSchema()
                                                    .setName(Field.RIGHT)
                                                    .setType("FLOAT")
                                                    .setMode("REQUIRED"),
                                            new TableFieldSchema()
                                                    .setName(Field.BOTTOM)
                                                    .setType("FLOAT")
                                                    .setMode("REQUIRED")
                                    ))
                    )
            );
        }

    }

    @Override
    public Iterable<KV<BQDestination, TableRow>> process(String gcsURI, StreamingAnnotateVideoResponse response) {
        StreamingVideoAnnotationResults annotationResults = response.getAnnotationResults();
        int numberOfAnnotations = annotationResults.getObjectAnnotationsCount();
        if (numberOfAnnotations == 0) {
            return null;
        }
        counter.inc(numberOfAnnotations);
        Collection<KV<BQDestination, TableRow>> result = new ArrayList<>(numberOfAnnotations);
        for (ObjectTrackingAnnotation annotation : annotationResults.getObjectAnnotationsList()) {
            TableRow row = ProcessorUtils.startRow(gcsURI);
            row.set(Field.CONFIDENCE, annotation.getConfidence());
            row.set(Field.ENTITY, annotation.hasEntity() ? annotation.getEntity().getDescription() : "NOT_FOUND");
            List<TableRow> frames = new ArrayList<>(annotation.getFramesCount());
            annotation
                .getFramesList()
                .forEach(
                    frame -> {
                        TableRow frameRow = new TableRow();
                        NormalizedBoundingBox normalizedBoundingBox = frame.getNormalizedBoundingBox();
                        frameRow.set(Field.TIME_OFFSET, frame.getTimeOffset());
                        frameRow.set(Field.LEFT, normalizedBoundingBox.getLeft());
                        frameRow.set(Field.TOP, normalizedBoundingBox.getTop());
                        frameRow.set(Field.RIGHT, normalizedBoundingBox.getRight());
                        frameRow.set(Field.BOTTOM, normalizedBoundingBox.getBottom());
                        frames.add(frameRow);
                    });
            row.put(Field.FRAMES, frames);
            LOG.debug("Processing {}", row);
            result.add(KV.of(destination, row));
        }
        return result;
    }

    @Override
    public TableDetails destinationTableDetails() {
        return TableDetails.create("Google Video Intelligence API object tracking annotations",
                new Clustering().setFields(Collections.singletonList(Field.GCS_URI_FIELD)),
                new TimePartitioning().setField(Field.TIMESTAMP_FIELD), new SchemaProducer());
    }
}
