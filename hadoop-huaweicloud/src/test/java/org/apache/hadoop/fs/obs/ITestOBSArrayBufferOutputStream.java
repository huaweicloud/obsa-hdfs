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

import static org.apache.hadoop.fs.obs.OBSConstants.FAST_UPLOAD_BUFFER;
import static org.apache.hadoop.fs.obs.OBSConstants.FAST_UPLOAD_BUFFER_ARRAY;
import static org.apache.hadoop.fs.obs.OBSConstants.FAST_UPLOAD_BUFFER_ARRAY_FIRST_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.fs.obs.OBSConstants.MULTIPART_MIN_SIZE;
import static org.apache.hadoop.fs.obs.OBSConstants.MULTIPART_SIZE;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;

/**
 * Tests small file upload functionality for {@link OBSBlockOutputStream} with
 * the blocks buffered in byte arrays.
 * <p>
 * File sizes are kept small to reduce test duration on slow connections;
 * multipart tests are kept in scale tests.
 */
@Deprecated
@RunWith(Parameterized.class)
public class ITestOBSArrayBufferOutputStream extends AbstractOBSTestBase {
    private static final int BLOCK_SIZE = 256 * 1024;

    private static byte[] dataset;

    private static String testRootPath =
        OBSTestUtils.generateUniqueTestPath();

    private boolean calcMd5;

    @Parameterized.Parameters
    public static Collection calcMd5() {
        return Arrays.asList(false, true);
    }

    public ITestOBSArrayBufferOutputStream(boolean calcMd5) {
        this.calcMd5 = calcMd5;
    }

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Override
    protected Configuration createConfiguration() {
        Configuration conf = super.createConfiguration();
        OBSTestUtils.disableFilesystemCaching(conf);
        conf.setInt(MULTIPART_SIZE, MULTIPART_MIN_SIZE);
        conf.setBoolean(OBSConstants.FAST_UPLOAD, true);
        conf.set(FAST_UPLOAD_BUFFER, getBlockOutputBufferName());
        conf.setBoolean(OBSConstants.OUTPUT_STREAM_ATTACH_MD5, calcMd5);
        return conf;
    }

    @After
    public void tearDown() throws Exception {
        if (getFileSystem() != null) {
            getFileSystem().delete(new Path(testRootPath), true);
        }
    }

    private Path getTestPath(String relativePath) {
        return new Path(testRootPath + "/" + relativePath);
    }

    protected String getBlockOutputBufferName() {
        return FAST_UPLOAD_BUFFER_ARRAY;
    }

    @Test
    // array内存缓存，首个缓存块大小为1M
    public void testArrayFirstBufferSize() throws Exception {
        Path dest = getTestPath("testMockRetryError");
        FSDataOutputStream stream = getFileSystem().create(dest, true);
        OBSBlockOutputStream obs
            = (OBSBlockOutputStream) stream.getWrappedStream();
        OBSDataBlocks.ByteArrayBlock block
            = (OBSDataBlocks.ByteArrayBlock) obs.getActiveBlock();
        assertTrue(String.format("Array first buffer must 1MB. real (%d)",
            block.firstBlockSize()),
            block.firstBlockSize()
                == FAST_UPLOAD_BUFFER_ARRAY_FIRST_BLOCK_SIZE_DEFAULT);
        if (stream != null) {
            stream.close();
        }
    }

    @Test
    // 上传0字节的文件
    public void testZeroByteUpload() throws IOException {
        verifyUpload("0", 0);
    }

    @Test
    // 上传1K大小文件
    public void testRegularUpload() throws IOException {
        verifyUpload("regular", 1024);
    }

    @Test
    // 流关闭后再写数据，抛IOException
    public void testArrayWriteAfterStreamClose() throws IOException {
        Path dest = getTestPath("testWriteAfterStreamClose");
        describe(" testWriteAfterStreamClose");
        FSDataOutputStream stream = getFileSystem().create(dest, true);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        try {
            stream.write(data);
            stream.close();
            boolean hasException = false;
            try {
                stream.write(data);
            } catch (IOException e) {
                hasException = true;
            }
            assertTrue(hasException);
        } finally {
            IOUtils.closeStream(stream);
        }
    }

    @Test
    // 流关闭后，activeBlock置为空
    public void testBlocksClosed() throws Throwable {
        Path dest = getTestPath("testBlocksClosed");
        describe(" testBlocksClosed");
        FSDataOutputStream stream = getFileSystem().create(dest, true);
        byte[] data = ContractTestUtils.dataset(16, 'a', 26);
        stream.write(data);
        stream.close();
        OBSBlockOutputStream obsStream =
            (OBSBlockOutputStream) stream.getWrappedStream();
        assertEquals(null, obsStream.getActiveBlock());
    }

    private void verifyUpload(String name, int fileSize) throws IOException {
        Path dest = getTestPath(name);
        describe(name + " upload to " + dest);
        ContractTestUtils.createAndVerifyFile(
            getFileSystem(),
            dest,
            fileSize);
    }

    /**
     * Create a factory for used in mark/reset tests.
     *
     * @param fileSystem source FS
     * @return the factory
     */
    protected OBSDataBlocks.BlockFactory createFactory(
        OBSFileSystem fileSystem) {
        return new OBSDataBlocks.ByteArrayBlockFactory(fileSystem);
    }

    private void markAndResetDatablock(OBSDataBlocks.BlockFactory factory)
        throws Exception {
        OBSDataBlocks.DataBlock block = factory.create(1, BLOCK_SIZE);
        dataset = ContractTestUtils.dataset(BLOCK_SIZE, 0, 256);
        block.write(dataset, 0, dataset.length);
        InputStream stream = (InputStream) block.startUpload();
        assertNotNull(stream);
        assertTrue("Mark not supported in " + stream, stream.markSupported());
        assertEquals(0, stream.read());
        stream.mark(BLOCK_SIZE);
        // read a lot
        long l = 0;
        while (stream.read() != -1) {
            // do nothing
            l++;
        }
        stream.reset();
        assertEquals(1, stream.read());
    }

    @Test
    // byte array流支持mark和reset操作
    public void testMarkReset() throws Throwable {
        markAndResetDatablock(createFactory(getFileSystem()));
    }

}
