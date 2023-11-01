package org.apache.hadoop.fs.obs;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.apache.hadoop.fs.obs.input.InputPolicyFactory;
import org.apache.hadoop.fs.obs.input.InputPolicys;
import org.apache.hadoop.fs.obs.input.OBSMemArtsCCInputStream;
import org.apache.hadoop.fs.obs.mock.MockMemArtsCCClient;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Arrays;

public class ITestOBSMemArtsCCInputStreamBufIO {
    private OBSFileSystem fs;

    private static final Logger LOG = LoggerFactory.getLogger(ITestOBSMemArtsCCInputStreamBufIO.class);

    private static String testRootPath = OBSTestUtils.generateUniqueTestPath();

    private MockMemArtsCCClient mockMemArtsCCClient;

    private int bufSize;

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @Rule
    public Timeout testTimeout = new Timeout(30 * 60 * 1000);

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    @Before
    public void setUp() throws Exception {
        Configuration conf = OBSContract.getConfiguration(null);
        conf.setClass(OBSConstants.OBS_METRICS_CONSUMER, MockMetricsConsumer.class, BasicMetricsConsumer.class);
        conf.setBoolean(OBSConstants.METRICS_SWITCH, true);
        conf.set(OBSConstants.READAHEAD_POLICY, OBSConstants.READAHEAD_POLICY_MEMARTSCC);
        bufSize = 8192;
        conf.setInt("fs.obs.memartscc.buffer.size", bufSize);
        conf.set("fs.obs.memartscc.inputstream.buffer.type", "bind");
        this.fs = OBSTestUtils.createTestFileSystem(conf);

        mockMemArtsCCClient = new MockMemArtsCCClient(fs, false, this.fs.getBucket());
        mockMemArtsCCClient.init("", "");

        InputPolicyFactory inputPolicyFactory = InputPolicys.createFactory(OBSConstants.READAHEAD_POLICY_MEMARTSCC);

        // mock memartscc client
        Field ccClient = OBSFileSystem.class.getDeclaredField("memArtsCCClient");
        ccClient.setAccessible(true);
        ccClient.set(this.fs, mockMemArtsCCClient);

        // mock input policy factory
        Field fInputPolicyFactory = OBSFileSystem.class.getDeclaredField("inputPolicyFactory");
        fInputPolicyFactory.setAccessible(true);
        fInputPolicyFactory.set(this.fs, inputPolicyFactory);
    }

    private void changeReadAheadRange(long readAheadRange) throws NoSuchFieldException, IllegalAccessException {
        Field rRange = OBSFileSystem.class.getDeclaredField("readAheadRange");
        rRange.setAccessible(true);
        rRange.set(this.fs, readAheadRange);
        return;
    }

    private void changeState(OBSMemArtsCCInputStream is, OBSMemArtsCCInputStream.State state)
        throws NoSuchFieldException, IllegalAccessException {
        Field fstate = OBSMemArtsCCInputStream.class.getDeclaredField("state");
        fstate.setAccessible(true);
        fstate.set(is, state);
        return;
    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
            fs.delete(new Path(testRootPath), true);
        }
    }

    private Path setPath(String path) {
        if (path.startsWith("/")) {
            return new Path(testRootPath + path);
        } else {
            return new Path(testRootPath + "/" + path);
        }
    }

    private static void genTestBuffer(byte[] buffer, long lenth) {
        for (int i = 0; i < lenth; i++) {
            buffer[i] = (byte) (i % 255);
        }
    }

    private int fullReadAndCheck(InputStream is, int offset, int size, boolean onebyteRead) throws IOException {
        byte[] buf = new byte[size];
        int bytesRead = 0;
        int off = 0;
        int _byte = 0;
        if (onebyteRead) {
            while (off < size) {
                _byte = is.read();
                if (_byte == -1) {
                    break;
                }
                if (_byte >= 0) {
                    buf[off] = (byte) _byte;
                    off ++;
                }
            }
        } else {
            do {
                off += bytesRead;
                bytesRead = is.read(buf, off, size - off);
            } while (bytesRead > 0);
        }

        byte[] testBuffer = new byte[256];
        genTestBuffer(testBuffer, 256);

        byte[] equalSizeBuffer = new byte[(int) size];

        int start = (offset % 256);
        for (int i = 0; i < equalSizeBuffer.length; i++) {
            equalSizeBuffer[i] = testBuffer[start % 256];
            start ++;
        }
        assertTrue(Arrays.equals(buf, equalSizeBuffer));
        return off;
    }

    @Test
    public void testRead1() throws NoSuchFieldException, IllegalAccessException, IOException {
        Path mread2OReadFile = setPath("/test/testRead1.txt");
        int size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, mread2OReadFile, size, 256, 255);
        LOG.info("5MB file created: testRead1.txt");
        FSDataInputStream fsDataInputStream = this.fs.open(mread2OReadFile);
        OBSMemArtsCCInputStream mis = (OBSMemArtsCCInputStream) fsDataInputStream.getWrappedStream();

        changeReadAheadRange(1 * 1024 * 1024);
        changeState(mis, OBSMemArtsCCInputStream.State.NEW);
        int readsize = 512 * 1024;
        int offset = 0;
        // read 0.5M to open ORead stream
        int bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,false);
        assertTrue(bytesRead == readsize);
        offset += bytesRead;

        readsize = 1 * 1024 * 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertTrue(bytesRead == readsize);
        offset += bytesRead;


        readsize = 512 * 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, true);
        assertTrue(bytesRead == readsize);
        offset += bytesRead;

        mockMemArtsCCClient.setNextCCReadReturnCacheMiss();
        readsize = 1024 * 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, true);
        assertTrue(bytesRead == readsize);
        offset += bytesRead;
    }

    private void doOnceMixReadTest(Path testFile, int oneByteReadSize)
        throws IOException, NoSuchFieldException, IllegalAccessException {
        FSDataInputStream fsDataInputStream = this.fs.open(testFile);
        OBSMemArtsCCInputStream mis = (OBSMemArtsCCInputStream) fsDataInputStream.getWrappedStream();

        changeReadAheadRange(1 * 1024 * 1024);
        // one byte read
        int readsize = oneByteReadSize;
        int offset = 0;
        // read 3 byte
        int bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,true);
        assertTrue(bytesRead == readsize);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;

        if (bufSize <= 1024) {
            throw new IllegalArgumentException("buffer size should larger than 1K");
        }
        // read less than bufferSize
        readsize = bufSize - 1024;

        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,false);
        assertTrue(bytesRead == readsize);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;

        // read larger than bufferSize
        readsize = bufSize + 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,false);
        assertTrue(bytesRead == readsize);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;
    }

    @Test
    public void testMixRead() throws IOException, NoSuchFieldException, IllegalAccessException {
        Path testMixReadFile = setPath("/test/testMixRead.txt");
        int size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, testMixReadFile, size, 256, 255);
        LOG.info("5MB file created: testMixRead.txt");

        for (int i = 1; i < 20; i++) {
            doOnceMixReadTest(testMixReadFile, i);
        }
    }

    @Test
    public void testMixRead1() throws IOException, NoSuchFieldException, IllegalAccessException {
        Path testMixReadFile = setPath("/test/testMixRead1.txt");
        int size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, testMixReadFile, size, 256, 255);
        LOG.info("5MB file created: testMixRead1.txt");

        FSDataInputStream fsDataInputStream = this.fs.open(testMixReadFile);
        OBSMemArtsCCInputStream mis = (OBSMemArtsCCInputStream) fsDataInputStream.getWrappedStream();

        changeReadAheadRange(1 * 1024 * 1024);
        // one byte read
        int readsize = 512 * 1024;
        int offset = 0;
        // read 0.5M to open ORead stream
        int bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,true);
        assertTrue(bytesRead == readsize);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;

        if (bufSize <= 1024) {
            throw new IllegalArgumentException("buffer size should larger than 1K");
        }
        // read less than bufferSize
        readsize = bufSize - 1024;

        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,false);
        assertTrue(bytesRead == readsize);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;

        // read larger than bufferSize
        readsize = bufSize + 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,false);
        assertTrue(bytesRead == readsize);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;
    }

    @Test
    public void testReadTail() throws NoSuchFieldException, IllegalAccessException, IOException {
        Path mread2OReadFile = setPath("/test/testReadTail.txt");
        int size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs,
            mread2OReadFile, size, 256, 255);
        LOG.info("5MB file created: testReadTail.txt");
        FSDataInputStream fsDataInputStream = this.fs.open(mread2OReadFile);
        OBSMemArtsCCInputStream mis = (OBSMemArtsCCInputStream) fsDataInputStream.getWrappedStream();

        changeReadAheadRange(1 * 1024 * 1024);

        int offset = (size - 8);
        fsDataInputStream.seek(offset);
        int readsize = 8;
        int bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,true);
        assertTrue(bytesRead == readsize);

        offset = (size - 6024);
        fsDataInputStream.seek(offset);
        readsize = 6024 - 8;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,true);
        assertTrue(bytesRead == readsize);

        offset = (size - 6024);
        fsDataInputStream.seek(offset);
        readsize = 6024 - 8;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,false);
        assertTrue(bytesRead == readsize);
    }

    @Test
    public void testSeekEscapeSeekRead() throws IOException, NoSuchFieldException, IllegalAccessException {
        Path testSeekEscapeSeekRead = setPath("/test/testSeekEscapeSeekRead.txt");
        int size = 5 * 1024 * 1024;
        ContractTestUtils.generateTestFile(this.fs, testSeekEscapeSeekRead, size, 256, 255);
        LOG.info("sim parquet file testSeekEscapeRead.txt in size {} created", size);
        FSDataInputStream fsDataInputStream = this.fs.open(testSeekEscapeSeekRead);
        OBSMemArtsCCInputStream mis = (OBSMemArtsCCInputStream) fsDataInputStream.getWrappedStream();

        changeReadAheadRange(1 * 1024 * 1024);
        changeState(mis, OBSMemArtsCCInputStream.State.MREAD);

        int offset = 0;
        int readsize = 4096;
        int bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertTrue(bytesRead == readsize);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;

        mockMemArtsCCClient.setNextCCReadReturnCacheMiss();
        readsize = 1 * 1024 * 1024;
        offset = 1 * 1024 * 1024;
        fsDataInputStream.seek(offset);
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertTrue(bytesRead == readsize);
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));

        offset = 0;
        readsize = 512 * 1024;
        fsDataInputStream.seek(offset);
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertTrue(bytesRead == readsize);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;
    }

    @Test
    public void testSimParquet() throws IOException, NoSuchFieldException, IllegalAccessException {
        Path testSimParquetFile = setPath("/test/testSimParquet.txt");
        int size = 137051553;
        ContractTestUtils.generateTestFile(this.fs, testSimParquetFile, size, 256, 255);
        LOG.info("sim parquet file testSimParquet.txt in size {} created", size);
        FSDataInputStream fsDataInputStream = this.fs.open(testSimParquetFile);
        OBSMemArtsCCInputStream mis = (OBSMemArtsCCInputStream) fsDataInputStream.getWrappedStream();

        changeReadAheadRange(8 * 1024 * 1024);
        changeState(mis, OBSMemArtsCCInputStream.State.MREAD);

        int offset = 27895882;
        fsDataInputStream.seek(offset);
        int readsize = 22;
        int bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, true);
        assertTrue(bytesRead == readsize);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;

        readsize = 2016;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertTrue(bytesRead == readsize);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;

        mockMemArtsCCClient.setNextCCReadReturnCacheMiss();
        offset = 2235733;
        readsize = 8388608;
        fsDataInputStream.seek(offset);
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertTrue(bytesRead == 8 * 1024 * 1024);
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));
        offset += bytesRead;

        mockMemArtsCCClient.setNextCCReadReturnCacheMiss();
        readsize = 3005571;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertTrue(bytesRead == readsize);
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));
        offset += bytesRead;

        offset = 27895882;
        readsize = 2328035;
        fsDataInputStream.seek(offset);
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertTrue(bytesRead == readsize);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;

        offset = 127584000;
        readsize = 9467553;
        fsDataInputStream.seek(offset);
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertTrue(bytesRead == readsize);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;
    }
}
