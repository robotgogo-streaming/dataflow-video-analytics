package com.google.solutions.df.video.analytics.videointelligence;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.videointelligence.v1p3beta1.StreamingAnnotateVideoResponse;

import java.io.Serializable;

import com.google.solutions.df.video.analytics.bigquery.BQDestination;
import com.google.solutions.df.video.analytics.bigquery.TableDetails;
import org.apache.beam.sdk.values.KV;

/**
 * Implementors of this interface will process zero to many TableRows to persist to a specific
 * BigTable table.
 */
public interface AnnotateVideoResponseProcessor extends Serializable {

    /**
     * @param gcsURI annotation source
     * @param response from Google Cloud Video Intelligence API
     * @return key/value pair of a BigQuery destination and a TableRow to persist.
     */
    Iterable<KV<BQDestination, TableRow>> process(String gcsURI, StreamingAnnotateVideoResponse response);

    /**
     * @return details of the table to persist to.
     */
    TableDetails destinationTableDetails();
}