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
import com.google.common.collect.Lists;
import com.obs.services.exception.ObsException;
import com.obs.services.model.ObsObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.ProviderUtils;
import org.slf4j.Logger;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.hadoop.fs.obs.Constants.*;

/** Utility methods for OBS code. */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class OBSUtils {

  /**
   * Core property for provider path. Duplicated here for consistent code across Hadoop version:
   * {@value}.
   */
  static final String CREDENTIAL_PROVIDER_PATH = "hadoop.security.credential.provider.path";
  /** Reuse the OBSFileSystem log. */
  private static final Logger LOG = OBSFileSystem.LOG;

  private OBSUtils() {}

  /**
   * Extract an exception from a failed future, and convert to an IOE.
   *
   * @param operation operation which failed
   * @param path path operated on (may be null)
   * @param ee execution exception
   * @return an IOE which can be thrown
   */
  public static IOException extractException(String operation, String path, ExecutionException ee) {
    IOException ioe;
    Throwable cause = ee.getCause();
    if (cause instanceof ObsException) {
      ioe = translateException(operation, path, (ObsException) cause);
    } else if (cause instanceof IOException) {
      ioe = (IOException) cause;
    } else {
      ioe = new IOException(operation + " failed: " + cause, cause);
    }
    return ioe;
  }

  /**
   * Create a files status instance from a listing.
   *
   * @param keyPath path to entry
   * @param summary summary from OBS
   * @param blockSize block size to declare.
   * @param owner owner of the file
   * @return a status entry
   */
  public static OBSFileStatus createFileStatus(
      Path keyPath, ObsObject summary, long blockSize, String owner) {
    if (objectRepresentsDirectory(
        summary.getObjectKey(), summary.getMetadata().getContentLength())) {
      return new OBSFileStatus(true, keyPath, owner);
    } else {
      return new OBSFileStatus(
          summary.getMetadata().getContentLength(),
          dateToLong(summary.getMetadata().getLastModified()),
          keyPath,
          blockSize,
          owner);
    }
  }

  /**
   * Predicate: does the object represent a directory?.
   *
   * @param name object name
   * @param size object size
   * @return true if it meets the criteria for being an object
   */
  public static boolean objectRepresentsDirectory(final String name, final long size) {
    return !name.isEmpty() && name.charAt(name.length() - 1) == '/' && size == 0L;
  }

  /**
   * Date to long conversion. Handles null Dates that can be returned by OBS by returning 0
   *
   * @param date date from OBS query
   * @return timestamp of the object
   */
  public static long dateToLong(final Date date) {
    if (date == null) {
      return 0L;
    }

    return date.getTime();
  }

  /**
   * Return the access key and secret for OBS API use. Credentials may exist in configuration,
   * within credential providers or indicated in the UserInfo of the name URI param.
   *
   * @param name the URI for which we need the access keys.
   * @param conf the Configuration object to interrogate for keys.
   * @return OBSAccessKeys
   * @throws IOException problems retrieving passwords from KMS.
   */
  public static OBSLoginHelper.Login getOBSAccessKeys(URI name, Configuration conf)
      throws IOException {
    OBSLoginHelper.Login login = OBSLoginHelper.extractLoginDetailsWithWarnings(name);
    Configuration c =
        ProviderUtils.excludeIncompatibleCredentialProviders(conf, OBSFileSystem.class);
    String accessKey = getPassword(c, ACCESS_KEY, login.getUser());
    String secretKey = getPassword(c, SECRET_KEY, login.getPassword());
    String sessionToken = getPassword(c, SESSION_TOKEN, login.getToken());
    return new OBSLoginHelper.Login(accessKey, secretKey, sessionToken);
  }

  /**
   * Get a password from a configuration, or, if a value is passed in, pick that up instead.
   *
   * @param conf configuration
   * @param key key to look up
   * @param val current value: if non empty this is used instead of querying the configuration.
   * @return a password or "".
   * @throws IOException on any problem
   */
  static String getPassword(Configuration conf, String key, String val) throws IOException {
    return StringUtils.isEmpty(val) ? lookupPassword(conf, key, "") : val;
  }

  /**
   * Get a password from a configuration/configured credential providers.
   *
   * @param conf configuration
   * @param key key to look up
   * @param defVal value to return if there is no password
   * @return a password or the value in {@code defVal}
   * @throws IOException on any problem
   */
  static String lookupPassword(Configuration conf, String key, String defVal) throws IOException {
    try {
      final char[] pass = conf.getPassword(key);
      return pass != null ? new String(pass).trim() : defVal;
    } catch (IOException ioe) {
      throw new IOException("Cannot find password option " + key, ioe);
    }
  }

  /**
   * String information about a summary entry for debug messages.
   *
   * @param summary summary object
   * @return string value
   */
  public static String stringify(ObsObject summary) {
    StringBuilder builder = new StringBuilder(summary.getObjectKey().length() + 100);
    builder.append(summary.getObjectKey()).append(' ');
    builder.append("size=").append(summary.getMetadata().getContentLength());
    return builder.toString();
  }

  /**
   * Get a integer option >= the minimum allowed value.
   *
   * @param conf configuration
   * @param key key to look up
   * @param defVal default value
   * @param min minimum value
   * @return the value
   * @throws IllegalArgumentException if the value is below the minimum
   */
  static int intOption(Configuration conf, String key, int defVal, int min) {
    int v = conf.getInt(key, defVal);
    Preconditions.checkArgument(
        v >= min, String.format("Value of %s: %d is below the minimum value %d", key, v, min));
    LOG.debug("Value of {} is {}", key, v);
    return v;
  }

  /**
   * Get a long option >= the minimum allowed value.
   *
   * @param conf configuration
   * @param key key to look up
   * @param defVal default value
   * @param min minimum value
   * @return the value
   * @throws IllegalArgumentException if the value is below the minimum
   */
  static long longOption(Configuration conf, String key, long defVal, long min) {
    long v = conf.getLong(key, defVal);
    Preconditions.checkArgument(
        v >= min, String.format("Value of %s: %d is below the minimum value %d", key, v, min));
    LOG.debug("Value of {} is {}", key, v);
    return v;
  }

  /**
   * Get a long option >= the minimum allowed value, supporting memory prefixes K,M,G,T,P.
   *
   * @param conf configuration
   * @param key key to look up
   * @param defVal default value
   * @param min minimum value
   * @return the value
   * @throws IllegalArgumentException if the value is below the minimum
   */
  static long longBytesOption(Configuration conf, String key, long defVal, long min) {
    long v = conf.getLongBytes(key, defVal);
    Preconditions.checkArgument(
        v >= min, String.format("Value of %s: %d is below the minimum value %d", key, v, min));
    LOG.debug("Value of {} is {}", key, v);
    return v;
  }

  /**
   * Get a size property from the configuration: this property must be at least equal to {@link
   * Constants#MULTIPART_MIN_SIZE}. If it is too small, it is rounded up to that minimum, and a
   * warning printed.
   *
   * @param conf configuration
   * @param property property name
   * @param defVal default value
   * @return the value, guaranteed to be above the minimum size
   */
  public static long getMultipartSizeProperty(Configuration conf, String property, long defVal) {
    long partSize = conf.getLongBytes(property, defVal);
    if (partSize < MULTIPART_MIN_SIZE) {
      LOG.warn("{} must be at least 5 MB; configured value is {}", property, partSize);
      partSize = MULTIPART_MIN_SIZE;
    }
    return partSize;
  }

  /**
   * Ensure that the long value is in the range of an integer.
   *
   * @param name property name for error messages
   * @param size original size
   * @return the size, guaranteed to be less than or equal to the max value of an integer.
   */
  public static int ensureOutputParameterInRange(String name, long size) {
    if (size > Integer.MAX_VALUE) {
      LOG.warn(
          "obs: {} capped to ~2.14GB" + " (maximum allowed size with current output mechanism)",
          name);
      return Integer.MAX_VALUE;
    } else {
      return (int) size;
    }
  }

  /**
   * Returns the public constructor of {@code cl} specified by the list of {@code args} or {@code
   * null} if {@code cl} has no public constructor that matches that specification.
   *
   * @param cl class
   * @param args constructor argument types
   * @return constructor or null
   */
  private static Constructor<?> getConstructor(Class<?> cl, Class<?>... args) {
    try {
      Constructor cons = cl.getDeclaredConstructor(args);
      return Modifier.isPublic(cons.getModifiers()) ? cons : null;
    } catch (NoSuchMethodException | SecurityException e) {
      return null;
    }
  }

  /**
   * Returns the public static method of {@code cl} that accepts no arguments and returns {@code
   * returnType} specified by {@code methodName} or {@code null} if {@code cl} has no public static
   * method that matches that specification.
   *
   * @param cl class
   * @param returnType return type
   * @param methodName method name
   * @return method or null
   */
  private static Method getFactoryMethod(Class<?> cl, Class<?> returnType, String methodName) {
    try {
      Method m = cl.getDeclaredMethod(methodName);
      if (Modifier.isPublic(m.getModifiers())
          && Modifier.isStatic(m.getModifiers())
          && returnType.isAssignableFrom(m.getReturnType())) {
        return m;
      } else {
        return null;
      }
    } catch (NoSuchMethodException | SecurityException e) {
      return null;
    }
  }

  /**
   * Propagates bucket-specific settings into generic OBS configuration keys. This is done by
   * propagating the values of the form {@code fs.obs.bucket.${bucket}.key} to {@code fs.obs.key},
   * for all values of "key" other than a small set of unmodifiable values.
   *
   * <p>The source of the updated property is set to the key name of the bucket property, to aid in
   * diagnostics of where things came from.
   *
   * <p>Returns a new configuration. Why the clone? You can use the same conf for different
   * filesystems, and the original values are not updated.
   *
   * <p>The {@code fs.obs.impl} property cannot be set, nor can any with the prefix {@code
   * fs.obs.bucket}.
   *
   * <p>This method does not propagate security provider path information from the OBS property into
   * the Hadoop common provider: callers must call {@link
   * #patchSecurityCredentialProviders(Configuration)} explicitly.
   *
   * @param source Source Configuration object.
   * @param bucket bucket name. Must not be empty.
   * @return a (potentially) patched clone of the original.
   */
  public static Configuration propagateBucketOptions(Configuration source, String bucket) {

    Preconditions.checkArgument(StringUtils.isNotEmpty(bucket), "bucket");
    final String bucketPrefix = FS_OBS_BUCKET_PREFIX + bucket + '.';
    LOG.debug("Propagating entries under {}", bucketPrefix);
    final Configuration dest = new Configuration(source);
    for (Map.Entry<String, String> entry : source) {
      final String key = entry.getKey();
      // get the (unexpanded) value.
      final String value = entry.getValue();
      if (!key.startsWith(bucketPrefix) || bucketPrefix.equals(key)) {
        continue;
      }
      // there's a bucket prefix, so strip it
      final String stripped = key.substring(bucketPrefix.length());
      if (stripped.startsWith("bucket.") || "impl".equals(stripped)) {
        // tell user off
        LOG.debug("Ignoring bucket option {}", key);
      } else {
        // propagate the value, building a new origin field.
        // to track overwrites, the generic key is overwritten even if
        // already matches the new one.
        final String generic = FS_OBS_PREFIX + stripped;
        LOG.debug("Updating {}", generic);
        dest.set(generic, value, key);
      }
    }
    return dest;
  }

  /**
   * Patch the security credential provider information in {@link #CREDENTIAL_PROVIDER_PATH} with
   * the providers listed in {@link Constants#OBS_SECURITY_CREDENTIAL_PROVIDER_PATH}.
   *
   * <p>This allows different buckets to use different credential files.
   *
   * @param conf configuration to patch
   */
  static void patchSecurityCredentialProviders(Configuration conf) {
    Collection<String> customCredentials =
        conf.getStringCollection(OBS_SECURITY_CREDENTIAL_PROVIDER_PATH);
    Collection<String> hadoopCredentials = conf.getStringCollection(CREDENTIAL_PROVIDER_PATH);
    if (!customCredentials.isEmpty()) {
      List<String> all = Lists.newArrayList(customCredentials);
      all.addAll(hadoopCredentials);
      String joined = StringUtils.join(all, ',');
      LOG.debug("Setting {} to {}", CREDENTIAL_PROVIDER_PATH, joined);
      conf.set(
          CREDENTIAL_PROVIDER_PATH, joined, "patch of " + OBS_SECURITY_CREDENTIAL_PROVIDER_PATH);
    }
  }

  /**
   * Close the Closeable objects and <b>ignore</b> any Exception or null pointers. (This is the
   * SLF4J equivalent of that in {@code IOUtils}).
   *
   * @param log the log to log at debug level. Can be null.
   * @param closeables the objects to close
   */
  public static void closeAll(Logger log, java.io.Closeable... closeables) {
    for (java.io.Closeable c : closeables) {
      if (c != null) {
        try {
          if (log != null) {
            log.debug("Closing {}", c);
          }
          c.close();
        } catch (Exception e) {
          if (log != null && log.isDebugEnabled()) {
            log.debug("Exception in closing {}", c, e);
          }
        }
      }
    }
  }
  /*
  ------------------------------------OBS UTILS------------------------------------------------
   */

  /**
   * Get low level details of a huawei OBS exception for logging; multi-line.
   *
   * @param e exception
   * @return string details
   */
  public static String stringify(ObsException e) {
    StringBuilder builder =
        new StringBuilder(
            String.format(
                "request id: %s, response code: %d, error code: %s, hostid: %s, message: %s",
                e.getErrorRequestId(),
                e.getResponseCode(),
                e.getErrorCode(),
                e.getErrorHostId(),
                e.getErrorMessage()));
    return builder.toString();
  }

  /**
   * Translate an exception raised in an operation into an IOException. HTTP error codes are
   * examined and can be used to build a more specific response.
   *
   * @param operation operation
   * @param path path operated on (may be null)
   * @param exception amazon exception raised
   * @return an IOE which wraps the caught exception.
   */
  @SuppressWarnings("ThrowableInstanceNeverThrown")
  public static IOException translateException(
      String operation, String path, ObsException exception) {
    String message =
        String.format("%s%s: status [%d] - request id [%s] - error code [%s] - error message [%s] - trace :%s ", operation, path != null ? (" on " + path) : "",
                exception.getResponseCode(), exception.getErrorRequestId(), exception.getErrorCode(), exception.getErrorMessage(), exception);

    IOException ioe;

    int status = exception.getResponseCode();
    switch (status) {
      case 301:
        message =
            String.format(
                "Received permanent redirect response , status [%d] - request id [%s] - error code [%s] - message [%s]", exception.getResponseCode(),
                    exception.getErrorRequestId(), exception.getErrorCode(), exception.getErrorMessage());
        ioe = new OBSIOException(message, exception);
        break;
        // permissions
      case 401:
      case 403:
        ioe = new AccessDeniedException(path, null, message);
        ioe.initCause(exception);
        break;

        // the object isn't there
      case 404:
      case 410:
        ioe = new FileNotFoundException(message);
        ioe.initCause(exception);
        break;

        // out of range. This may happen if an object is overwritten with
        // a shorter one while it is being read.
      case 416:
        ioe = new EOFException(message);
        break;

      default:
        // no specific exit code. Choose an IOE subclass based on the
        // class
        // of the caught exception
        ioe = new OBSIOException(message, exception);
        break;
    }
    return ioe;
  }

  /**
   * Translate an exception raised in an operation into an IOException. The specific type of
   * IOException depends on the class of {@link ObsException} passed in, and any status codes
   * included in the operation. That is: HTTP error codes are examined and can be used to build a
   * more specific response.
   *
   * @param operation operation
   * @param path path operated on (must not be null)
   * @param exception amazon exception raised
   * @return an IOE which wraps the caught exception.
   */
  public static IOException translateException(
      String operation, Path path, ObsException exception) {
    return translateException(operation, path.toString(), exception);
  }
}
