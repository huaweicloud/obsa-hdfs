package org.apache.hadoop.fs.obs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link OBSDataBlocks}.
 */
public class ITestOBSDataBlocks extends Assert {
    private OBSFileSystem fs;

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @Rule
    public Timeout testTimeout = new Timeout(30 * 1000);

    private List<String> factoryPolicies = Arrays.asList(OBSConstants.FAST_UPLOAD_BUFFER_DISK,
        OBSConstants.FAST_UPLOAD_BUFFER_ARRAY, OBSConstants.FAST_UPLOAD_BYTEBUFFER);

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Before
    public void nameThread() throws IOException {
        Thread.currentThread().setName("JUnit");
        Configuration conf = OBSContract.getConfiguration(null);
        fs = OBSTestUtils.createTestFileSystem(conf);
    }

    /**
     * Test the {@link OBSDataBlocks.ByteBufferBlockFactory}. That code
     * implements an input stream over a ByteBuffer, and has to return the
     * buffer to the pool after the read complete.
     * <p>
     * This test verifies the basic contract of the process.
     */
    @Test
    // 测试基于byte buffer写缓存的读写基本功能
    public void testByteBufferIO() throws Throwable {
        OBSDataBlocks.ByteBufferBlockFactory factory = new OBSDataBlocks.ByteBufferBlockFactory(fs);
        int limit = 128;
        OBSDataBlocks.ByteBufferBlock block = factory.create(1, limit);
        assertOutstandingBufferCount(factory, 1);

        byte[] buffer = ContractTestUtils.toAsciiByteArray("test data");
        int bufferLen = buffer.length;
        block.write(buffer, 0, bufferLen);
        assertEquals(bufferLen, block.dataSize());
        assertEquals(limit - bufferLen, block.remainingCapacity());
        assertTrue(block.hasCapacity(64));
        assertTrue(block.hasCapacity(limit - bufferLen));

        // now start the write
        OBSDataBlocks.ByteBufferBlock.ByteBufferInputStream stream =
            (OBSDataBlocks.ByteBufferBlock.ByteBufferInputStream) block.startUpload();
        assertTrue(stream.markSupported());
        assertTrue(stream.hasRemaining());
        int expected = bufferLen;
        assertEquals(expected, stream.available());

        assertEquals('t', stream.read());
        stream.mark(limit);
        expected--;
        assertEquals(expected, stream.available());

        // read into a byte array with an offset
        int offset = 5;
        byte[] data = new byte[limit];
        int read = stream.read(data, offset, 2);
        assertEquals(2, read);
        assertEquals('e', data[offset]);
        assertEquals('s', data[offset + 1]);
        expected -= 2;
        assertEquals(expected, stream.available());

        // read to end
        byte[] remains = new byte[limit];
        int value;
        int index = 0;
        while ((value = stream.read()) >= 0) {
            remains[index++] = (byte) value;
        }
        assertEquals(expected, index);
        assertEquals('a', remains[--index]);

        assertEquals(0, stream.available());
        assertTrue(!stream.hasRemaining());

        // go the mark point
        stream.reset();
        assertEquals('e', stream.read());

        // when the stream is closed, the data should be returned
        stream.close();
        assertOutstandingBufferCount(factory, 1);
        block.close();
        assertOutstandingBufferCount(factory, 0);
        stream.close();
        assertOutstandingBufferCount(factory, 0);
    }

    @Test
    public void testNotCalcChecksum() throws IOException {
        for (String factory : factoryPolicies) {
            Configuration conf = OBSContract.getConfiguration(null);
            OBSDataBlocks.DataBlock dataBlock =
                startUploadDataBlock(factory, conf, ContractTestUtils.toAsciiByteArray("test data"));
            assertNull("factory is " + factory, dataBlock.getChecksum());
            assertEquals("factory is " + factory, OBSDataBlocks.ChecksumType.NONE, dataBlock.getChecksumType());
            dataBlock.close();
        }
    }

    @Test
    public void testCalcMD5() throws IOException {
        for (String factory : factoryPolicies) {
            Configuration conf = OBSContract.getConfiguration(null);
            conf.setBoolean(OBSConstants.OUTPUT_STREAM_ATTACH_MD5, true);
            OBSDataBlocks.DataBlock dataBlock =
                    startUploadDataBlock(factory, conf, ContractTestUtils.toAsciiByteArray("test data"));
            assertEquals("factory is " + factory, "63M6AMDJ0zbmVpGjerVCkw==", dataBlock.getChecksum());
            assertEquals("factory is " + factory, OBSDataBlocks.ChecksumType.MD5, dataBlock.getChecksumType());
            dataBlock.close();
        }
    }

    @Test
    public void testCalcMD5UseNewConfig() throws IOException {
        for (String factory : factoryPolicies) {
            Configuration conf = OBSContract.getConfiguration(null);
            conf.set(OBSConstants.FAST_UPLOAD_CHECKSUM_TYPE, OBSConstants.FAST_UPLOAD_CHECKSUM_TYPE_MD5);
            OBSDataBlocks.DataBlock dataBlock =
                startUploadDataBlock(factory, conf, ContractTestUtils.toAsciiByteArray("test data"));
            assertEquals("factory is " + factory, "63M6AMDJ0zbmVpGjerVCkw==", dataBlock.getChecksum());
            assertEquals("factory is " + factory, OBSDataBlocks.ChecksumType.MD5, dataBlock.getChecksumType());
            dataBlock.close();
        }
    }

    @Test
    public void testCalcSha256() throws IOException {
        for (String factory : factoryPolicies) {
            Configuration conf = OBSContract.getConfiguration(null);
            conf.set(OBSConstants.FAST_UPLOAD_CHECKSUM_TYPE, OBSConstants.FAST_UPLOAD_CHECKSUM_TYPE_SHA256);
            OBSDataBlocks.DataBlock dataBlock =
                startUploadDataBlock(factory, conf, ContractTestUtils.toAsciiByteArray("test data"));
            assertEquals("factory is " + factory, "916f0027a575074ce72a331777c3478d6513f786a591bd892da1a577bf2335f9",
                dataBlock.getChecksum());
            assertEquals("factory is " + factory, OBSDataBlocks.ChecksumType.SHA256, dataBlock.getChecksumType());
            dataBlock.close();
        }
    }

    @Test
    public void testWrongChecksumType() {
        for (String factory : factoryPolicies) {
            Configuration conf = OBSContract.getConfiguration(null);
            conf.set(OBSConstants.FAST_UPLOAD_CHECKSUM_TYPE, "wrongType");
            try {
                startUploadDataBlock(factory, conf, ContractTestUtils.toAsciiByteArray("test data"));
                fail("should be throw exception when type is wrong. factoryPolicy: " + factory);
            } catch (IOException e) {
                assertTrue(e.getMessage(), e.getMessage().startsWith("Unsupported fast upload checksum type"));
            }
        }
    }

    private OBSDataBlocks.DataBlock startUploadDataBlock(String factoryName, Configuration conf, byte[] data)
        throws IOException {
        OBSFileSystem fileSystem = OBSTestUtils.createTestFileSystem(conf);
        OBSDataBlocks.BlockFactory factory = OBSDataBlocks.createFactory(fileSystem, factoryName);
        OBSDataBlocks.DataBlock dataBlock = factory.create(1, 128);
        dataBlock.write(data, 0, data.length);
        dataBlock.startUpload();
        return dataBlock;
    }

    /**
     * Assert the number of buffers active for a block factory.
     *
     * @param factory factory
     * @param expectedCount expected count.
     */
    private static void assertOutstandingBufferCount(OBSDataBlocks.ByteBufferBlockFactory factory, int expectedCount) {
        assertEquals("outstanding buffers in " + factory, expectedCount, factory.getOutstandingBufferCount());
    }

}
