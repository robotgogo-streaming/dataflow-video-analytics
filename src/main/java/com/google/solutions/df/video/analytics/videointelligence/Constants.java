package com.google.solutions.df.video.analytics.videointelligence;

public interface Constants {

    interface Field {
        String GCS_URI_FIELD = "gcs_uri";
        String TIMESTAMP_FIELD = "transaction_timestamp";
        String METADATA = "metadata";
        String ENTITY = "entity";
        String SEGMENTS = "segments";
        String START_TIME_OFFSET = "start_time_offset";
        String END_TIME_OFFSET = "end_time_offset";
        String FRAMES = "frames";  // TODO: Rename to "frame_data"?
        String TIME_OFFSET = "time_offset";
        String CONFIDENCE = "confidence";
        String LEFT = "left";
        String TOP = "top";
        String RIGHT = "right";
        String BOTTOM = "bottom";
    }

}
