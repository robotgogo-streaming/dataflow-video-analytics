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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.StorageObject;
//import com.google.cloud.storage.Blob;
//import com.google.cloud.storage.Storage;
//import com.google.cloud.storage.StorageOptions;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.solutions.df.video.analytics.videointelligence.Constants.Field;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Various utility functions used by processors
 */
public class ProcessorUtils {

  static void setMetadataFieldsSchema(List<TableFieldSchema> fields, Set<String> metadataKeys) {
    if (!metadataKeys.isEmpty()) {
      List<TableFieldSchema> metadataFields = new ArrayList<>();
      for (String key : metadataKeys) {
        metadataFields.add(
             new TableFieldSchema()
                   .setName(key)
                   .setType("STRING")
        );
      }
      fields.add(new TableFieldSchema()
              .setName(Field.METADATA)
              .setType("RECORD")
              .setFields(metadataFields)
          );
    }
  }



  /**
   * Creates a TableRow and populates with two fields used in all processors: {@link
   * Constants.Field#GCS_URI_FIELD} and {@link Constants.Field#TIMESTAMP_FIELD}
   *
   * @return new TableRow
   */
  static TableRow startRow(String gcsURI) {
    TableRow row = new TableRow();
    row.put(Field.GCS_URI_FIELD, gcsURI);
    row.put(Field.TIMESTAMP_FIELD, getTimeStamp());
    return row;
  }

  static void addMetadataValues(TableRow row, String gcsURI, Set<String> metadataKeys) {
    URI uri;
    try {
      uri = new URI(gcsURI);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    // Fetch metadata from GCS
    Storage storageClient = getStorageClient();
    Storage.Objects.Get getObject;
    StorageObject object;
    try {
      getObject = storageClient.objects().get(uri.getHost(), uri.getPath().substring(1));
      object = getObject.execute();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Map<String, String> metadata = object.getMetadata();

    // Add metadata to the row, if any
    TableRow metadataRow = new TableRow();
    if (metadata != null) {
      for (String key : metadataKeys) {
        String value = metadata.get(key);
        if (value != null) {
          metadataRow.put(key, value);
        }
      }
    }
    if (!metadataRow.isEmpty()) {
      row.put(Field.METADATA, metadataRow);
    }
  }

  private static final String APPLICATION_NAME = "my-app-name"; // FIXME
  private static Storage storageService;
  private static Storage getStorageClient() {
      if (null == storageService) {
        HttpTransport httpTransport;
        GoogleCredentials credentials;
        try {
          httpTransport = GoogleNetHttpTransport.newTrustedTransport();
          credentials = GoogleCredentials.getApplicationDefault().createScoped(StorageScopes.CLOUD_PLATFORM);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);
        JacksonFactory jacksonFactory = JacksonFactory.getDefaultInstance();
        storageService = new Storage.Builder(httpTransport, jacksonFactory, requestInitializer)
          .setApplicationName(APPLICATION_NAME).build();
      }
      return storageService;
  }

  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

  /**
   * Formats the current timestamp in BigQuery compliant format
   */
  public static String getTimeStamp() {
    return TIMESTAMP_FORMATTER.print(Instant.now().toDateTime(DateTimeZone.UTC));
  }
}
