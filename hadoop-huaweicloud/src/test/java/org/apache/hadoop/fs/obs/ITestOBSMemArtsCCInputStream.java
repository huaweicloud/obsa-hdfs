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

public class ITestOBSMemArtsCCInputStream {
    private OBSFileSystem fs;

    private static final Logger LOG = LoggerFactory.getLogger(ITestOBSMemArtsCCInputStream.class);

    private static String testRootPath = OBSTestUtils.generateUniqueTestPath();

    private MockMemArtsCCClient mockMemArtsCCClient;

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
        conf.setInt("fs.obs.memartscc.buffer.size", 1);
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
    public void testNew2ORead() throws NoSuchFieldException, IllegalAccessException, IOException {
        Path mread2OReadFile = setPath("/test/testNew2ORead.txt");
        int size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, mread2OReadFile, size, 256, 255);
        LOG.info("5MB file created: testMRead2ORead.txt");
        FSDataInputStream fsDataInputStream = this.fs.open(mread2OReadFile);
        OBSMemArtsCCInputStream mis = (OBSMemArtsCCInputStream) fsDataInputStream.getWrappedStream();

        changeReadAheadRange(1 * 1024 * 1024);
        changeState(mis, OBSMemArtsCCInputStream.State.NEW);
        assertTrue("input state should be 'New'", mis.getState().equals(OBSMemArtsCCInputStream.State.NEW));
        int readsize = 512 * 1024;
        int offset = 0;
        // read 0.5M to open ORead stream
        int bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,false);
        assertTrue(bytesRead == readsize);
        offset += bytesRead;
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));
    }

    @Test
    public void testStayInORead() throws NoSuchFieldException, IllegalAccessException, IOException {
        Path mread2OReadFile = setPath("/test/testStayInORead.txt");
        int size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, mread2OReadFile, size, 256, 255);
        LOG.info("5MB file created: testMRead2ORead.txt");
        FSDataInputStream fsDataInputStream = this.fs.open(mread2OReadFile);
        OBSMemArtsCCInputStream mis = (OBSMemArtsCCInputStream) fsDataInputStream.getWrappedStream();

        changeReadAheadRange(1 * 1024 * 1024);
        changeState(mis, OBSMemArtsCCInputStream.State.NEW);
        assertTrue("input state should be 'New'", mis.getState().equals(OBSMemArtsCCInputStream.State.NEW));
        int readsize = 512 * 1024;
        int offset = 0;
        // read 0.5M to open ORead stream
        int bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertTrue(bytesRead == readsize);
        offset += bytesRead;
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));

        // read 256K to stay in ORead
        readsize = 256 * 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertTrue(bytesRead == readsize);
        offset += bytesRead;
        assertTrue("should stay in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));
    }

    @Test
    public void testNew2ORead2MRead2ORead() throws NoSuchFieldException, IllegalAccessException, IOException {
        Path mread2OReadFile = setPath("/test/testNew2ORead2MRead2ORead.txt");
        int size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, mread2OReadFile, size, 256, 255);
        LOG.info("5MB file created: testMRead2ORead.txt");
        FSDataInputStream fsDataInputStream = this.fs.open(mread2OReadFile);
        OBSMemArtsCCInputStream mis = (OBSMemArtsCCInputStream) fsDataInputStream.getWrappedStream();

        changeReadAheadRange(1 * 1024 * 1024);
        changeState(mis, OBSMemArtsCCInputStream.State.NEW);
        assertTrue("input state should be 'New'", mis.getState().equals(OBSMemArtsCCInputStream.State.NEW));
        int readsize = 512 * 1024;
        int offset = 0;
        // read 0.5M to open ORead stream
        int bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertTrue(bytesRead == readsize);
        offset += bytesRead;
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));
        // read 1M to transfer to MRead
        readsize = 1 * 1024 * 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertTrue(bytesRead == readsize);
        offset += bytesRead;
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));

        // read 1M to stay in MRead
        readsize = 1 * 1024 * 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertTrue(bytesRead == readsize);
        offset += bytesRead;
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));

        // set cache miss
        this.mockMemArtsCCClient.setNextCCReadReturnCacheMiss();

        // read 512KB to transfer to ORead
        readsize = 512 * 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertTrue(bytesRead == readsize);
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));

    }

    @Test
    public void testMRead2ORead() throws NoSuchFieldException, IllegalAccessException, IOException {
        Path mread2OReadFile = setPath("/test/testMRead2ORead.txt");
        int size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, mread2OReadFile, size, 256, 255);
        LOG.info("5MB file created: testMRead2ORead.txt");
        FSDataInputStream fsDataInputStream = this.fs.open(mread2OReadFile);
        OBSMemArtsCCInputStream mis = (OBSMemArtsCCInputStream) fsDataInputStream.getWrappedStream();

        changeReadAheadRange(1 * 1024 * 1024);
        changeState(mis, OBSMemArtsCCInputStream.State.MREAD);

        int readsize = 512 * 1024;
        int offset = 0;

        int bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertTrue(bytesRead == readsize);
        offset += bytesRead;
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));

        // set cache miss
        this.mockMemArtsCCClient.setNextCCReadReturnCacheMiss();

        // read 512KB to transfer to ORead
        readsize = 512 * 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertTrue(bytesRead == readsize);
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));
    }

    @Test
    public void testLazySeek2MRead() throws NoSuchFieldException, IllegalAccessException, IOException {
        Path mread2OReadFile = setPath("/test/testLazySeek2MRead.txt");
        int size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, mread2OReadFile, size, 256, 255);
        LOG.info("5MB file created: testMRead2ORead.txt");
        FSDataInputStream fsDataInputStream = this.fs.open(mread2OReadFile);
        OBSMemArtsCCInputStream mis = (OBSMemArtsCCInputStream) fsDataInputStream.getWrappedStream();

        changeReadAheadRange(1 * 1024 * 1024);
        changeState(mis, OBSMemArtsCCInputStream.State.NEW);
        assertTrue("input state should be 'New'", mis.getState().equals(OBSMemArtsCCInputStream.State.NEW));
        int readsize = 512 * 1024;
        int offset = 0;
        // read 0.5M to open ORead stream
        int bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,false);
        assertTrue(bytesRead == readsize);
        offset += bytesRead;
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));
        // ORead 100 one byte read
        readsize = 256;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, true);
        assertTrue(bytesRead == readsize);
        offset += bytesRead;
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));

        offset += 2 * 1024 * 1024;
        fsDataInputStream.seek(offset);
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));

        // read 0.5M to transfer to MRead
        readsize = 512 * 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertTrue(bytesRead == readsize);
        offset += bytesRead;
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));

        // MRead 100 one byte read
        readsize = 256;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, true);
        assertTrue(bytesRead == readsize);
        offset += bytesRead;
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
    }

    @Test
    public void testSmallRandomRead() throws IOException, NoSuchFieldException, IllegalAccessException {
        Path mread2OReadFile = setPath("/test/testSmallRandomRead.txt");
        int size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, mread2OReadFile, size, 256, 255);
        LOG.info("5MB file created: testMRead2ORead.txt");
        FSDataInputStream fsDataInputStream = this.fs.open(mread2OReadFile);
        OBSMemArtsCCInputStream mis = (OBSMemArtsCCInputStream) fsDataInputStream.getWrappedStream();

        changeReadAheadRange(1 * 1024 * 1024);
        changeState(mis, OBSMemArtsCCInputStream.State.NEW);
        assertTrue("input state should be 'New'", mis.getState().equals(OBSMemArtsCCInputStream.State.NEW));
        int offset = 0;
        int readsize = 1;
        int bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,true);
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));
        offset += bytesRead;

        readsize = 10;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,true);
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));
        offset += bytesRead;

        readsize = 100;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,true);
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));
        offset += bytesRead;

        readsize = 1000;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,true);
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));
        offset += bytesRead;

        readsize = 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,true);
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));
        offset += bytesRead;

        // seek and reopen to mread
        offset = 35;
        fsDataInputStream.seek(offset);
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));

        readsize = 100;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,true);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;

        readsize = 1000;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,true);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;

        readsize = 1;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,true);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;

        readsize = 5;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,true);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;

        // another seek
        offset = 37 * 1024;
        fsDataInputStream.seek(offset);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));

        readsize = 5;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,true);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;

        readsize = 1;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,true);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;

        readsize = 29;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,true);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;

        // set cache miss
        this.mockMemArtsCCClient.setNextCCReadReturnCacheMiss();
        readsize = 10;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,true);
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));
        offset += bytesRead;

        this.mockMemArtsCCClient.setNextCCReadReturnCacheMiss();
        readsize = 100;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,true);
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));
        offset += bytesRead;
    }

    @Test
    public void testReadBackward() throws IOException, NoSuchFieldException, IllegalAccessException {
        Path mread2OReadFile = setPath("/test/testReadBackward.txt");
        int size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, mread2OReadFile, size, 256, 255);
        LOG.info("5MB file created: testMRead2ORead.txt");
        FSDataInputStream fsDataInputStream = this.fs.open(mread2OReadFile);
        OBSMemArtsCCInputStream mis = (OBSMemArtsCCInputStream) fsDataInputStream.getWrappedStream();

        changeReadAheadRange(1 * 1024 * 1024);
        changeState(mis, OBSMemArtsCCInputStream.State.NEW);
        assertTrue("input state should be 'New'", mis.getState().equals(OBSMemArtsCCInputStream.State.NEW));
        int offset = 0;
        int readsize = 1 * 1024 * 1024;
        int bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,false);
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));
        offset += bytesRead;

        readsize = 1 * 1024 * 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,false);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;

        this.mockMemArtsCCClient.setNextCCReadReturnCacheMiss();
        readsize = 1 * 1024 * 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,false);
        assertTrue("should in ORead State", mis.getState().equals(OBSMemArtsCCInputStream.State.OREAD));
        offset += bytesRead;

        offset = 0;
        fsDataInputStream.seek(offset);
        readsize = 1 * 1024 * 1024 + 512 * 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertTrue("should in MRead State", mis.getState().equals(OBSMemArtsCCInputStream.State.MREAD));
        offset += bytesRead;
    }
}
