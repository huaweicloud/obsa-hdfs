/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.obs;

import com.google.common.base.Preconditions;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.*;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.fs.obs.OBSUtils.translateException;

/**
 * Helper for an ongoing write operation.
 *
 * <p>It hides direct access to the OBS API from the output stream, and is a location where the
 * object upload process can be evolved/enhanced.
 *
 * <p>Features
 *
 * <ul>
 *   <li>Methods to create and submit requests to OBS, so avoiding all direct interaction with the
 *       OBS APIs.
 *   <li>Some extra preflight checks of arguments, so failing fast on errors.
 *   <li>Callbacks to let the FS know of events in the output stream upload process.
 * </ul>
 *
 * Each instance of this state is unique to a single output stream.
 */
class OBSWriteOperationHelper {
  public static final Logger LOG = LoggerFactory.getLogger(OBSWriteOperationHelper.class);
  /** Owning filesystem. */
  private final OBSFileSystem owner;
  /** Configuration of the owner. This is a reference, not a copy. */
  private final Configuration conf;
  /** Bucket of the owner FS. */
  private final String bucket;

  private final ObsClient obs;

  protected OBSWriteOperationHelper(OBSFileSystem owner, Configuration conf) {
    this.owner = owner;
    this.conf = conf;
    this.bucket = owner.getBucket();
    this.obs = owner.getObsClient();
  }

  /**
   * Create a {@link PutObjectRequest} request. If {@code length} is set, the metadata is configured
   * with the size of the upload.
   *
   * @param inputStream source data.
   * @param length size, if known. Use -1 for not known
   * @return the request
   */
  PutObjectRequest newPutRequest(String destKey, InputStream inputStream, long length) {
    PutObjectRequest request =
        owner.newPutObjectRequest(destKey, newObjectMetadata(length), inputStream);
    return request;
  }

  /**
   * Create a {@link PutObjectRequest} request to upload a file.
   *
   * @param sourceFile source file
   * @return the request
   */
  PutObjectRequest newPutRequest(String destKey, File sourceFile) {
    int length = (int) sourceFile.length();
    PutObjectRequest request =
        owner.newPutObjectRequest(destKey, newObjectMetadata(length), sourceFile);
    return request;
  }

  /** Callback on a successful write. */
  void writeSuccessful(String destKey) {
    owner.finishedWrite(destKey);
  }

  /**
   * Callback on a write failure.
   *
   * @param e Any exception raised which triggered the failure.
   */
  void writeFailed(Exception e) {
    LOG.debug("Write to {} failed", this, e);
  }

  /**
   * Create a new object metadata instance. Any standard metadata headers are added here, for
   * example: encryption.
   *
   * @param length size, if known. Use -1 for not known
   * @return a new metadata instance
   */
  public ObjectMetadata newObjectMetadata(long length) {
    return owner.newObjectMetadata(length);
  }

  /**
   * Start the multipart upload process.
   *
   * @return the upload result containing the ID
   * @throws IOException IO problem
   */
  String initiateMultiPartUpload(String destKey) throws IOException {
    LOG.debug("Initiating Multipart upload");
    final InitiateMultipartUploadRequest initiateMPURequest =
        new InitiateMultipartUploadRequest(bucket, destKey);
    initiateMPURequest.setAcl(owner.getCannedACL());
    // TODO object length
    initiateMPURequest.setMetadata(newObjectMetadata(-1));
    if (owner.getSse().isSseCEnable()) {
      initiateMPURequest.setSseCHeader(owner.getSse().getSseCHeader());
    } else if (owner.getSse().isSseKmsEnable()) {
      initiateMPURequest.setSseKmsHeader(owner.getSse().getSseKmsHeader());
    }
    try {
      return obs.initiateMultipartUpload(initiateMPURequest).getUploadId();
    } catch (ObsException ace) {
      throw translateException("Initiate MultiPartUpload", destKey, ace);
    }
  }

  /**
   * Complete a multipart upload operation.
   *
   * @param uploadId multipart operation Id
   * @param partETags list of partial uploads
   * @return the result
   * @throws ObsException on problems.
   */
  CompleteMultipartUploadResult completeMultipartUpload(
      String destKey, String uploadId, List<PartEtag> partETags) throws ObsException {
    Preconditions.checkNotNull(uploadId);
    Preconditions.checkNotNull(partETags);
    Preconditions.checkArgument(!partETags.isEmpty(), "No partitions have been uploaded");
    LOG.debug("Completing multipart upload {} with {} parts", uploadId, partETags.size());
    // a copy of the list is required, so that the OBS SDK doesn't
    // attempt to sort an unmodifiable list.
    return obs.completeMultipartUpload(
        new CompleteMultipartUploadRequest(bucket, destKey, uploadId, new ArrayList<>(partETags)));
  }

  /**
   * Abort a multipart upload operation.
   *
   * @param uploadId multipart operation Id
   * @throws ObsException on problems. Immediately execute
   */
  void abortMultipartUpload(String destKey, String uploadId) throws ObsException {
    LOG.debug("Aborting multipart upload {}", uploadId);
    obs.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, destKey, uploadId));
  }
  /**
   * Create request for uploading one part of a multipart task
   *
   * @param uploadId
   * @param partNumber
   * @param size
   * @param uploadStream
   * @param sourceFile
   * @return
   */
  UploadPartRequest newUploadPartRequest(
      String destKey,
      String uploadId,
      int partNumber,
      int size,
      InputStream uploadStream,
      File sourceFile) {
    Preconditions.checkNotNull(uploadId);

    Preconditions.checkArgument((uploadStream != null) ^ (sourceFile != null), "Data source");
    Preconditions.checkArgument(size > 0, "Invalid partition size %s", size);
    Preconditions.checkArgument(partNumber > 0 && partNumber <= 10000);

    LOG.debug("Creating part upload request for {} #{} size {}", uploadId, partNumber, size);
    UploadPartRequest request = new UploadPartRequest();
    request.setUploadId(uploadId);
    request.setBucketName(bucket);
    request.setObjectKey(destKey);
    request.setPartSize((long) size);
    request.setPartNumber(partNumber);
    if (uploadStream != null) {
      // there's an upload stream. Bind to it.
      request.setInput(uploadStream);
    } else {
      // or file
      request.setFile(sourceFile);
    }
    if (owner.getSse().isSseCEnable()) {
      request.setSseCHeader(owner.getSse().getSseCHeader());
    }
    return request;
  }

  public String toString(String destKey) {
    final StringBuilder sb = new StringBuilder("{bucket=").append(bucket);
    sb.append(", key='").append(destKey).append('\'');
    sb.append('}');
    return sb.toString();
  }

  /**
   * PUT an object directly (i.e. not via the transfer manager).
   *
   * @param putObjectRequest the request
   * @return the upload initiated
   * @throws IOException on problems
   */
  PutObjectResult putObject(PutObjectRequest putObjectRequest) throws IOException {
    try {
      return owner.putObjectDirect(putObjectRequest);
    } catch (ObsException e) {
      throw translateException("put", putObjectRequest.getObjectKey(), e);
    }
  }
}
