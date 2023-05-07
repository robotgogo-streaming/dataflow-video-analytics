package com.google.solutions.df.video.analytics.videointelligence;

import java.util.*;

import com.google.api.services.bigquery.model.*;
import com.google.cloud.videointelligence.v1p3beta1.*;
import com.google.common.collect.ImmutableList;
import com.google.solutions.df.video.analytics.bigquery.BQDestination;
import com.google.solutions.df.video.analytics.bigquery.TableDetails;
import com.google.solutions.df.video.analytics.bigquery.TableSchemaProducer;
import com.google.solutions.df.video.analytics.videointelligence.Constants.Field;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LabelAnnotationProcessor implements AnnotateVideoResponseProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(LabelAnnotationProcessor.class);
    private final BQDestination destination;
    private final Set<String> metadataKeys;
    public final static Counter counter =
            Metrics.counter(AnnotateVideoResponseProcessor.class, "numberOfLabelAnnotations");

    /**
     * Creates a processor and specifies the table id to persist to.
     */
    public LabelAnnotationProcessor(String tableId, Set<String> metadataKeys) {
        this.destination = new BQDestination(tableId);
        this.metadataKeys = metadataKeys;
    }

    private static class SchemaProducer implements TableSchemaProducer {

        private static final long serialVersionUID = 1L;
        private final Set<String> metadataKeys;

        SchemaProducer(Set<String> metadataKeys) {
            this.metadataKeys = metadataKeys;
        }

        @Override
        public TableSchema getTableSchema() {
            ArrayList<TableFieldSchema> fields = new ArrayList<>();
            fields.add(
                new TableFieldSchema()
                .setName(Field.GCS_URI_FIELD)
                .setType("STRING")
                .setMode("REQUIRED"));
            fields.add(
                new TableFieldSchema()
                    .setName(Field.TIMESTAMP_FIELD)
                    .setType("TIMESTAMP")
                    .setMode("REQUIRED"));
            fields.add(
                new TableFieldSchema()
                    .setName(Field.ENTITY)
                    .setType("STRING")
                    .setMode("REQUIRED"));
            fields.add(
                new TableFieldSchema()
                    .setName(Field.FRAMES).setType("RECORD")
                    .setMode("REPEATED")
                    .setFields(ImmutableList.of(
                        new TableFieldSchema()
                            .setName(Field.CONFIDENCE)
                            .setType("FLOAT")
                            .setMode("REQUIRED"),
                        new TableFieldSchema()
                            .setName(Field.TIME_OFFSET)
                            .setType("INT64")
                            .setMode("REQUIRED")
                    )));
//                            new TableFieldSchema()
//                                    .setName(Field.SEGMENTS).setType("RECORD")
//                                    .setMode("REPEATED")
//                                    .setFields(ImmutableList.of(
//                                            new TableFieldSchema()
//                                                    .setName(Field.CONFIDENCE)
//                                                    .setType("FLOAT")
//                                                    .setMode("REQUIRED"),
//                                            new TableFieldSchema()
//                                                    .setName(Field.START_TIME_OFFSET)
//                                                    .setType("INT64")
//                                                    .setMode("REQUIRED"),
//                                            new TableFieldSchema()
//                                                    .setName(Field.END_TIME_OFFSET)
//                                                    .setType("INT64")
//                                                    .setMode("REQUIRED")
//                                    ))

            ProcessorUtils.setMetadataFieldsSchema(fields, metadataKeys);

            return new TableSchema().setFields(fields);
        }

    }



    @Override
    public Iterable<KV<BQDestination, TableRow>> process(String gcsURI, StreamingAnnotateVideoResponse response) {
        StreamingVideoAnnotationResults annotationResults = response.getAnnotationResults();
        int numberOfAnnotations = annotationResults.getLabelAnnotationsCount();
        if (numberOfAnnotations == 0) {
            return null;
        }
        counter.inc(numberOfAnnotations);
        Collection<KV<BQDestination, TableRow>> result = new ArrayList<>(numberOfAnnotations);
        for (LabelAnnotation annotation : annotationResults.getLabelAnnotationsList()) {
            TableRow row = ProcessorUtils.startRow(gcsURI);
            row.set(Field.ENTITY, annotation.hasEntity() ? annotation.getEntity().getDescription() : "NOT_FOUND");  // FIXME: Seems like sometimes it's an empty string?

//            LOG.info("gcsURI: {}  Entity: {}  Num segments: {}  Num frames: {}", gcsURI, annotation.getEntity().getDescription(), segments.size(), annotation.getFramesCount());

            List<TableRow> frames = new ArrayList<>(annotation.getFramesCount());
            annotation
                    .getFramesList()
                    .forEach(
                            frame -> {
                                TableRow frameRow = new TableRow();
                                frameRow.set(Field.CONFIDENCE, frame.getConfidence());
                                frameRow.set(Field.TIME_OFFSET, frame.getTimeOffset().getSeconds());
                                frames.add(frameRow);
                        });
            row.put(Field.FRAMES, frames);

            ProcessorUtils.addMetadataValues(row, gcsURI, metadataKeys);

//            List<TableRow> segments = new ArrayList<>(annotation.getSegmentsCount());
//            annotation
//                .getSegmentsList()
//                .forEach(
//                    segment -> {
//                        TableRow segmentRow = new TableRow();
//                        segmentRow.set(Field.CONFIDENCE, segment.getConfidence());
//                        segmentRow.set(Field.START_TIME_OFFSET, segment.getSegment().getStartTimeOffset().getSeconds());
//                        segmentRow.set(Field.END_TIME_OFFSET, segment.getSegment().getEndTimeOffset().getSeconds());
//                        segments.add(segmentRow);
//                    });
//            row.put(Field.SEGMENTS, segments);


            LOG.debug("Processing {}", row);
            result.add(KV.of(destination, row));
        }
        return result;
    }


    @Override
    public TableDetails destinationTableDetails() {
        return TableDetails.create("Google Video Intelligence API label annotations",
                new Clustering().setFields(Collections.singletonList(Field.GCS_URI_FIELD)),
                new TimePartitioning().setField(Field.TIMESTAMP_FIELD), new SchemaProducer(metadataKeys));
    }
}
