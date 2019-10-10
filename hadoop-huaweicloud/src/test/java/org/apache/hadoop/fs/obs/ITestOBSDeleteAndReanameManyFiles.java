/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.obs;

import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists;
import static org.apache.hadoop.fs.contract.ContractTestUtils.bandwidth;
import static org.apache.hadoop.fs.contract.ContractTestUtils.verifyReceivedData;
import static org.apache.hadoop.fs.obs.OBSTestConstants.DEFAULT_OPERATION_COUNT;
import static org.apache.hadoop.fs.obs.OBSTestConstants.KEY_OPERATION_COUNT;
import static org.junit.Assert.assertEquals;

/**
 * Test some scalable operations related to file renaming and deletion.
 */
public class ITestOBSDeleteAndReanameManyFiles {
    private OBSFileSystem fs;
    Configuration conf;
    private static String testRootPath =
            OBSTestUtils.generateUniqueTestPath();
    private static final Logger LOG =
      LoggerFactory.getLogger(ITestOBSDeleteAndReanameManyFiles.class);


  @Before
  public void setUp() throws Exception {
      conf = new Configuration();
      fs = OBSTestUtils.createTestFileSystem(conf);
  }
    private Path getTestPath() {
        return new Path(testRootPath + "/test-obs");
    }
  @Test
  public void testBulkRenameAndDelete() throws Throwable {
    final Path scaleTestDir =new Path("testBulkRenameAndDelete");
    final Path srcDir = new Path(scaleTestDir, "1/2/3/src");
    final Path finalDir = new Path(scaleTestDir, "1/2/src1");
    final long count = conf.getLong(KEY_OPERATION_COUNT, DEFAULT_OPERATION_COUNT);

    boolean needCreat = true;
    ContractTestUtils.rm(fs, new Path("/1"), true, false);
    ContractTestUtils.rm(fs, scaleTestDir, true, false);
    fs.mkdirs(srcDir);
    fs.mkdirs(finalDir);

    int testBufferSize = fs.getConf()
        .getInt(ContractTestUtils.IO_CHUNK_BUFFER_SIZE,
            ContractTestUtils.DEFAULT_IO_CHUNK_BUFFER_SIZE);
    // use Executor to speed up file creation
    ExecutorService exec = Executors.newFixedThreadPool(16);
    final ExecutorCompletionService<Boolean> completionService =
        new ExecutorCompletionService<>(exec);
    try {
      final byte[] data = ContractTestUtils.dataset(testBufferSize, 'a', 'z');

      if (needCreat) {
          for (int i = 0; i < count; ++i) {
              final String fileName = "foo-" + i;
              completionService.submit(new Callable<Boolean>() {
                  @Override
                  public Boolean call() throws IOException {
                      ContractTestUtils.createFile(fs, new Path(srcDir, fileName), false, data);
                      return fs.exists(new Path(srcDir, fileName));
                  }
              });
          }
//          final Path srcSubDir = new Path(scaleTestDir, "1/2/3/src/sub");
//          for (int i = 0; i<2; ++i){
//              final String fileName = "foo1-"+i;
//              ContractTestUtils.createFile(fs, new Path(srcSubDir, fileName), false, data);
//          }
      }

      for (int i = 0; i < 999; ++i) {
        final Future<Boolean> future = completionService.take();
        try {
          if (!future.get()) {
            LOG.warn("cannot create file");
          }
        } catch (ExecutionException e) {
          LOG.warn("Error while uploading file", e.getCause());
          throw e;
        }
      }
    } finally {
      exec.shutdown();
    }


    int nSrcFiles = fs.listStatus(srcDir).length;


//    ObjectListing objects = fs.getObsClient.listObjects (request);
//
//    for (int i = 0; i < nSrcFiles; i++) {
//
//
//    }
    fs.rename(srcDir, finalDir);
    assertEquals(nSrcFiles, fs.listStatus(finalDir).length);
    ContractTestUtils.assertPathDoesNotExist(fs, "not deleted after rename",
        new Path(srcDir, "foo-" + 0));
    ContractTestUtils.assertPathDoesNotExist(fs, "not deleted after rename",
        new Path(srcDir, "foo-" + count / 2));
    ContractTestUtils.assertPathDoesNotExist(fs, "not deleted after rename",
        new Path(srcDir, "foo-" + (count - 1)));
    assertPathExists(fs, "not renamed to dest dir",
        new Path(finalDir, "foo-" + 0));
    assertPathExists(fs, "not renamed to dest dir",
        new Path(finalDir, "foo-" + count/2));
    assertPathExists(fs, "not renamed to dest dir",
        new Path(finalDir, "foo-" + (count-1)));

//    ContractTestUtils.assertDeleted(fs, finalDir, true, false);
  }

    @Test
    public void testListDelimiter() throws Throwable {
        final Path scaleTestDir =new Path("testBulkRenameAndDelete");
        final Path srcDir = new Path(scaleTestDir, "1/2/3/src");
        final Path finalDir = new Path(scaleTestDir, "1/2/src1");
        final long count = conf.getLong(KEY_OPERATION_COUNT, 10);

        boolean needCreat = true;
        ContractTestUtils.rm(fs, new Path("/1"), true, false);
        ContractTestUtils.rm(fs, scaleTestDir, true, false);
        fs.mkdirs(srcDir);
        fs.mkdirs(finalDir);

        int testBufferSize = fs.getConf()
                .getInt(ContractTestUtils.IO_CHUNK_BUFFER_SIZE,
                        ContractTestUtils.DEFAULT_IO_CHUNK_BUFFER_SIZE);
        // use Executor to speed up file creation
        ExecutorService exec = Executors.newFixedThreadPool(16);
        final ExecutorCompletionService<Boolean> completionService =
                new ExecutorCompletionService<>(exec);
        try {
            final byte[] data = ContractTestUtils.dataset(testBufferSize, 'a', 'z');

            if (needCreat) {
                for (int i = 0; i < count; ++i) {
                    final String fileName = "foo-" + i;
                    completionService.submit(new Callable<Boolean>() {
                        @Override
                        public Boolean call() throws IOException {
                            ContractTestUtils.createFile(fs, new Path(srcDir, fileName), false, data);
                            return fs.exists(new Path(srcDir, fileName));
                        }
                    });
                }
                final Path srcSub1Dir = new Path(scaleTestDir, "1/2/3/src/sub1");
                for (int i = 0; i < 5; ++i){
                    final String fileName = "foo1-"+i;
                    ContractTestUtils.createFile(fs, new Path(srcSub1Dir, fileName), false, data);
                }
                final Path srcSub2Dir = new Path(scaleTestDir, "1/2/3/src/sub1/sub2");
                for (int i = 0; i < 5; ++i){
                    final String fileName = "foo2-"+i;
                    ContractTestUtils.createFile(fs, new Path(srcSub2Dir, fileName), false, data);
                }
            }

            for (int i = 0; i < count; ++i) {
                final Future<Boolean> future = completionService.take();
                try {
                    if (!future.get()) {
                        LOG.warn("cannot create file");
                    }
                } catch (ExecutionException e) {
                    LOG.warn("Error while uploading file", e.getCause());
                    throw e;
                }
            }
        } finally {
            exec.shutdown();
        }
        String sreKey = pathToKey(srcDir);
        if (!sreKey.endsWith("/")) {
            sreKey = sreKey + "/";
        }
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(fs.getBucket());
        request.setPrefix(sreKey);
//        request.setDelimiter("/");
        request.setMaxKeys(1000);

        ObjectListing objects = fs.getObsClient().listObjects(request);

        for(ObsObject obsObject : objects.getObjects()){
            System.out.println("\t" + obsObject.getObjectKey());
            System.out.println("\t" + obsObject.getOwner());
        }


    }
  @Test
    public  void testHugeFileRename()throws Throwable{
      int testBufferSize = fs.getConf().getInt("io.chunk.buffer.size", 1024*1024);
      int modulus = fs.getConf().getInt("io.chunk.modulus.size", 128);
      long fileSize = 1024*1024*1024L;
      final Path srcDir = new Path(getTestPath(), "src");
      final Path finalDir = new Path(getTestPath(), "final");

        ContractTestUtils.rm(fs, getTestPath(), true, false);
        fs.mkdirs(srcDir);
        fs.mkdirs(finalDir);
//        String objectName = UUID.randomUUID().toString();
        Path objectPath = new Path(srcDir, "copy-test-file");
        Path renamePath = new Path(finalDir, "copy-test-file");
        boolean needUpload = true;
        ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();

        byte[] testBuffer = new byte[testBufferSize];

        for(int i = 0; i < testBuffer.length; ++i) {
            testBuffer[i] = (byte)(i % modulus);
        }

        long bytesWritten = 0L;

        Throwable var10 = null;

        long diff;
        if (needUpload) {
            OutputStream outputStream = fs.create(objectPath, false);
            try {
                while (bytesWritten < fileSize) {
                    diff = fileSize - bytesWritten;
                    if (diff < (long) testBuffer.length) {
                        outputStream.write(testBuffer, 0, (int) diff);
                        bytesWritten += diff;
                    } else {
                        outputStream.write(testBuffer);
                        bytesWritten += (long) testBuffer.length;
                    }
                }

                diff = bytesWritten;
            } catch (Throwable var21) {
                var10 = var21;
                throw var21;
            } finally {
                if (outputStream != null) {
                    if (var10 != null) {
                        try {
                            outputStream.close();
                        } catch (Throwable var20) {
                            var10.addSuppressed(var20);
                        }
                    } else {
                        outputStream.close();
                    }
                }

            }
        }

        fs.rename(srcDir, finalDir);
//        assertEquals(fileSize, diff);
        assertPathExists(fs, "not created successful", renamePath);
        timer.end("Time to write %d bytes", fileSize);
        bandwidth(timer, fileSize);

      try {
            verifyReceivedData(fs, renamePath, fileSize, testBufferSize, modulus);
      } finally {
            fs.delete(renamePath, false);
      }
  }
    @Test
    public  void testHugeFileCopy()throws Throwable{
        int testBufferSize = fs.getConf().getInt("io.chunk.buffer.size", 1024*1024);
        int modulus = fs.getConf().getInt("io.chunk.modulus.size", 128);
        long fileSize = 1024*1024*1024L;
        final Path srcDir = new Path(getTestPath(), "src");
        final Path finalDir = new Path(getTestPath(), "final");
        boolean needUpload = false;


//        String objectName = UUID.randomUUID().toString();
        Path objectPath = new Path(srcDir, "copy-test-file");
        Path renamePath = new Path(finalDir, "copy-test-file");

        byte[] testBuffer = new byte[testBufferSize];

        for(int i = 0; i < testBuffer.length; ++i) {
            testBuffer[i] = (byte)(i % modulus);
        }

        long bytesWritten = 0L;

        Throwable var10 = null;

        long diff;
        if (needUpload) {
            ContractTestUtils.rm(fs, getTestPath(), true, false);
            fs.mkdirs(srcDir);
            fs.mkdirs(finalDir);
            OutputStream outputStream = fs.create(objectPath, false);
            try {
                while (bytesWritten < fileSize) {
                    diff = fileSize - bytesWritten;
                    if (diff < (long) testBuffer.length) {
                        outputStream.write(testBuffer, 0, (int) diff);
                        bytesWritten += diff;
                    } else {
                        outputStream.write(testBuffer);
                        bytesWritten += (long) testBuffer.length;
                    }
                }

                diff = bytesWritten;
            } catch (Throwable var21) {
                var10 = var21;
                throw var21;
            } finally {
                if (outputStream != null) {
                    if (var10 != null) {
                        try {
                            outputStream.close();
                        } catch (Throwable var20) {
                            var10.addSuppressed(var20);
                        }
                    } else {
                        outputStream.close();
                    }
                }

            }
        }
        ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
//        String srcKey = fs.pathToKey(objectPath);
        for (int i = 1; i < 2; i++)
        {
            Path copyPath = new Path(finalDir, "copy-test-file-"+(i+1));
//            String desKey = fs.pathToKey(copyPath);
//            fs.copyFile(srcKey,desKey,fileSize);
        }

        LOG.debug("copy take times: {}",timer.end("Time to copy 100 times %d bytes", fileSize));
//        try {
//            verifyReceivedData(fs, renamePath, fileSize, testBufferSize, modulus);
//        } finally {
//            fs.delete(renamePath, false);
//        }
    }

    @Test
    public void testListing() throws Throwable {
        final Path scaleTestDir =new Path("/testBulkRenameAndDelete");
        final Path srcDir = new Path(scaleTestDir, "1/src");
        final Path finalDir = new Path(scaleTestDir, "1/2/src2");
        final long count = conf.getLong(KEY_OPERATION_COUNT, 1200);

        boolean needCreat = true;
        ContractTestUtils.rm(fs, new Path("/1"), true, false);
        ContractTestUtils.rm(fs, scaleTestDir, true, false);
        fs.mkdirs(srcDir);
        fs.mkdirs(finalDir);

        int testBufferSize = fs.getConf()
                .getInt(ContractTestUtils.IO_CHUNK_BUFFER_SIZE,
                        ContractTestUtils.DEFAULT_IO_CHUNK_BUFFER_SIZE);
        // use Executor to speed up file creation
        ExecutorService exec = Executors.newFixedThreadPool(16);
        final ExecutorCompletionService<Boolean> completionService =
                new ExecutorCompletionService<>(exec);
        try {
            final byte[] data = ContractTestUtils.dataset(testBufferSize, 'a', 'z');

            if (needCreat) {
                for (int i = 0; i < count; ++i) {
                    final String fileName = "foo-" + i;
                    completionService.submit(new Callable<Boolean>() {
                        @Override
                        public Boolean call() throws IOException {
                            ContractTestUtils.createFile(fs, new Path(srcDir, fileName), false, data);
                            return fs.exists(new Path(srcDir, fileName));
                        }
                    });
                }
                final Path srcSub1Dir = new Path(scaleTestDir, "1/src/sub1");
                for (int i = 0; i < 5; ++i){
                    final String fileName = "foo1-"+i;
                    ContractTestUtils.createFile(fs, new Path(srcSub1Dir, fileName), false, data);
                }
                final Path srcSub2Dir = new Path(scaleTestDir, "1/src/sub1/sub2");
                for (int i = 0; i < 5; ++i){
                    final String fileName = "foo2-"+i;
                    ContractTestUtils.createFile(fs, new Path(srcSub2Dir, fileName), false, data);
                }
            }

            for (int i = 0; i < count; ++i) {
                final Future<Boolean> future = completionService.take();
                try {
                    if (!future.get()) {
                        LOG.warn("cannot create file");
                    }
                } catch (ExecutionException e) {
                    LOG.warn("Error while uploading file", e.getCause());
                    throw e;
                }
            }
        } finally {
            exec.shutdown();
        }
        String sreKey = pathToKey(srcDir);
        if (!sreKey.endsWith("/")) {
            sreKey = sreKey + "/";
        }
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(fs.getBucket());
        request.setPrefix(sreKey);
        request.setDelimiter("/");
        request.setMaxKeys(1000);

        ObjectListing objects = fs.getObsClient().listObjects(request);

        while (true) {
            for (ObsObject obsObject : objects.getObjects()) {
                System.out.println("\t" + obsObject.getObjectKey());
            }

            if (!objects.isTruncated()){
                break;
            }

            objects = fs.continueListObjects(objects);
        }

    }
    private String pathToKey(Path path) {
        if (!path.isAbsolute()) {
            path = new Path(fs.getWorkingDirectory(), path);
        }

        if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
            return "";
        }

        return path.toUri().getPath().substring(1);
    }
}
