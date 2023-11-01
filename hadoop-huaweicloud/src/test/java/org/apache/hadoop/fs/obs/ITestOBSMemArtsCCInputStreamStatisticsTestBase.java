package org.apache.hadoop.fs.obs;

import static org.apache.hadoop.fs.obs.OBSTestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.obs.TrafficStatistics.TrafficType.Q;
import static org.apache.hadoop.fs.obs.TrafficStatistics.TrafficType.Q1;
import static org.apache.hadoop.fs.obs.TrafficStatistics.TrafficType.Q2;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

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
import org.junit.rules.Timeout;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class ITestOBSMemArtsCCInputStreamStatisticsTestBase {
    protected static final Logger LOG = LoggerFactory.getLogger(ITestOBSMemArtsCCInputStreamTrafficReport.class);

    private static final String testRootPath = OBSTestUtils.generateUniqueTestPath();

    // redirect System.out for testing
    protected final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

    private final PrintStream originalOut = System.out;

    @Rule
    public OBSTestRule testRule = new OBSTestRule();

    @Rule
    public Timeout testTimeout = new Timeout(30 * 60 * 1000);

    protected OBSFileSystem fs;

    protected MockMemArtsCCClient mockMemArtsCCClient;

    @BeforeClass
    public static void skipTestCheck() {
        Assume.assumeTrue(OBSContract.isContractTestEnabled());
    }

    private static void genTestBuffer(byte[] buffer, long length) {
        for (int i = 0; i < length; i++) {
            buffer[i] = (byte) (i % 255);
        }
    }

    @Before
    public void setUp() throws Exception {
        Configuration conf = OBSContract.getConfiguration(null);
        conf.setClass(OBSConstants.OBS_METRICS_CONSUMER, MockMetricsConsumer.class, BasicMetricsConsumer.class);
        conf.setBoolean(OBSConstants.METRICS_SWITCH, true);
        conf.set(OBSConstants.READAHEAD_POLICY, OBSConstants.READAHEAD_POLICY_MEMARTSCC);
        conf.set("fs.obs.memartscc.inputstream.buffer.type", "pool");
        conf.setBoolean(OBSConstants.MEMARTSCC_TRAFFIC_REPORT_ENABLE, true);
        fs = OBSTestUtils.createTestFileSystem(conf);

        mockMemArtsCCClient = new MockMemArtsCCClient(fs, false, this.fs.getBucket());
        mockMemArtsCCClient.init("", "");

        InputPolicyFactory inputPolicyFactory = InputPolicys.createFactory(OBSConstants.READAHEAD_POLICY_MEMARTSCC);

        // mock memartscc client
        Field ccClient = OBSFileSystem.class.getDeclaredField("memArtsCCClient");
        ccClient.setAccessible(true);
        ccClient.set(this.fs, mockMemArtsCCClient);

        Method method = OBSFileSystem.class.getDeclaredMethod("initTrafficReport", Configuration.class);
        method.setAccessible(true);
        method.invoke(this.fs, conf);

        // mock input policy factory
        Field fInputPolicyFactory = OBSFileSystem.class.getDeclaredField("inputPolicyFactory");
        fInputPolicyFactory.setAccessible(true);
        fInputPolicyFactory.set(this.fs, inputPolicyFactory);

        // redirect System.out
        System.setOut(new PrintStream(outContent));
    }

    protected void changeReadAheadRange(long readAheadRange) throws NoSuchFieldException, IllegalAccessException {
        Field rRange = OBSFileSystem.class.getDeclaredField("readAheadRange");
        rRange.setAccessible(true);
        rRange.set(this.fs, readAheadRange);
    }

    protected void changeState(OBSMemArtsCCInputStream is, OBSMemArtsCCInputStream.State state)
        throws NoSuchFieldException, IllegalAccessException {
        Field fstate = OBSMemArtsCCInputStream.class.getDeclaredField("state");
        fstate.setAccessible(true);
        fstate.set(is, state);
    }

    public void tearDownFS() throws Exception {
        if (fs != null) {
            fs.delete(new Path(testRootPath), true);
            fs.getSchemeStatistics().reset();
            fs.close();
        }
    }

    @After
    public void tearDown() throws Exception {
        // restore System.out
        System.setOut(originalOut);
        System.out.println(outContent);
        try {
            tearDownFS();
        } catch (IOException ignored) {
        }
    }

    protected Path setPath(String path) {
        if (path.startsWith("/")) {
            return new Path(testRootPath + path);
        } else {
            return new Path(testRootPath + "/" + path);
        }
    }

    protected int fullReadAndCheck(InputStream is, int offset, int size, boolean onebyteRead) throws IOException {
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
                    off++;
                }
            }
        } else {
            do {
                off += bytesRead;
                bytesRead = is.read(buf, off, size - off);
            } while (bytesRead > 0);
        }

        byte[] testBuffer = new byte[256];
        ITestOBSMemArtsCCInputStreamStatisticsTestBase.genTestBuffer(testBuffer, 256);

        byte[] equalSizeBuffer = new byte[(int) size];

        int start = (offset % 256);
        for (int i = 0; i < equalSizeBuffer.length; i++) {
            equalSizeBuffer[i] = testBuffer[start % 256];
            start++;
        }
        assertArrayEquals(buf, equalSizeBuffer);
        return off;
    }

    protected void runNew2ORead2MRead2ORead() throws IOException, NoSuchFieldException, IllegalAccessException {
        Path mread2OReadFile = setPath("/test/testNew2ORead2MRead2ORead.txt");
        int size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, mread2OReadFile, size, 256, 255);
        LOG.info("5MB file created: testMRead2ORead.txt");
        FSDataInputStream fsDataInputStream = this.fs.open(mread2OReadFile);
        OBSMemArtsCCInputStream mis = (OBSMemArtsCCInputStream) fsDataInputStream.getWrappedStream();

        changeReadAheadRange(1 * 1024 * 1024);
        changeState(mis, OBSMemArtsCCInputStream.State.NEW);
        assertEquals("input state should be 'New'", mis.getState(), OBSMemArtsCCInputStream.State.NEW);
        int readsize = 512 * 1024;
        int offset = 0;
        // read 0.5M to open ORead stream
        int bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertEquals(bytesRead, readsize);
        offset += bytesRead;
        assertEquals("should in ORead State", mis.getState(), OBSMemArtsCCInputStream.State.OREAD);
        // read 1M to transfer to MRead
        readsize = 1 * 1024 * 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertEquals(bytesRead, readsize);
        offset += bytesRead;
        assertEquals("should in MRead State", mis.getState(), OBSMemArtsCCInputStream.State.MREAD);

        // read 1M to stay in MRead
        readsize = 1 * 1024 * 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertEquals(bytesRead, readsize);
        offset += bytesRead;
        assertEquals("should in MRead State", mis.getState(), OBSMemArtsCCInputStream.State.MREAD);

        // set cache miss
        this.mockMemArtsCCClient.setNextCCReadReturnCacheMiss();

        // read 512KB to transfer to ORead
        readsize = 512 * 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertEquals(bytesRead, readsize);
        assertEquals("should in ORead State", mis.getState(), OBSMemArtsCCInputStream.State.OREAD);
    }

    protected void runLazySeek2MRead() throws IOException, NoSuchFieldException, IllegalAccessException {
        Path mread2OReadFile = setPath("/test/testLazySeek2MRead.txt");
        int size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, mread2OReadFile, size, 256, 255);
        LOG.info("5MB file created: testMRead2ORead.txt");
        FSDataInputStream fsDataInputStream = this.fs.open(mread2OReadFile);
        OBSMemArtsCCInputStream mis = (OBSMemArtsCCInputStream) fsDataInputStream.getWrappedStream();

        changeReadAheadRange(1 * 1024 * 1024);
        changeState(mis, OBSMemArtsCCInputStream.State.NEW);
        assertEquals("input state should be 'New'", mis.getState(), OBSMemArtsCCInputStream.State.NEW);
        int readsize = 512 * 1024;
        int offset = 0;
        // read 0.5M to open ORead stream
        int bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize,false);
        assertEquals(bytesRead, readsize);
        offset += bytesRead;
        assertEquals("should in ORead State", mis.getState(), OBSMemArtsCCInputStream.State.OREAD);

        readsize = 256;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, true);
        assertEquals(bytesRead, readsize);
        offset += bytesRead;
        assertEquals("should in ORead State", mis.getState(), OBSMemArtsCCInputStream.State.OREAD);

        offset += 2 * 1024 * 1024;
        fsDataInputStream.seek(offset);
        assertEquals("should in ORead State", mis.getState(), OBSMemArtsCCInputStream.State.OREAD);

        // read 0.5M to transfer to MRead
        readsize = 512 * 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertEquals(bytesRead, readsize);
        offset += bytesRead;
        assertEquals("should in MRead State", mis.getState(), OBSMemArtsCCInputStream.State.MREAD);

        readsize = 256;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, true);
        assertEquals(bytesRead, readsize);
        offset += bytesRead;
        assertEquals("should in ORead State", mis.getState(), OBSMemArtsCCInputStream.State.MREAD);
    }

    protected void runBackSeekAndRead() throws IOException, NoSuchFieldException, IllegalAccessException {
        Path testFile = setPath("/test/testBackSeekAndRead.txt");
        int size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, testFile, size, 256, 255);
        LOG.info("5MB file created: testMRead2ORead.txt");
        FSDataInputStream fsDataInputStream = this.fs.open(testFile);
        OBSMemArtsCCInputStream mis = (OBSMemArtsCCInputStream) fsDataInputStream.getWrappedStream();

        changeReadAheadRange(1 * 1024 * 1024);
        changeState(mis, OBSMemArtsCCInputStream.State.NEW);
        assertEquals("input state should be 'New'", mis.getState(), OBSMemArtsCCInputStream.State.NEW);
        int offset = 2 * 1024 * 1024;
        fsDataInputStream.seek(offset);

        int readsize = 512 * 1024;
        int bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertEquals(bytesRead, readsize);
        assertEquals("should in OREAD State", mis.getState(), OBSMemArtsCCInputStream.State.OREAD);

        // Seek to original position and read
        fsDataInputStream.seek(offset);
        readsize = 512 * 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertEquals(bytesRead, readsize);
    }

    protected void runSeekAndRead() throws IOException, NoSuchFieldException, IllegalAccessException {
        Path testFile = setPath("/test/testSeekAndRead.txt");
        int size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, testFile, size, 256, 255);
        LOG.info("5MB file created: testMRead2ORead.txt");
        FSDataInputStream fsDataInputStream = this.fs.open(testFile);
        OBSMemArtsCCInputStream mis = (OBSMemArtsCCInputStream) fsDataInputStream.getWrappedStream();

        changeReadAheadRange(1 * 1024 * 1024);
        changeState(mis, OBSMemArtsCCInputStream.State.NEW);
        assertEquals("input state should be 'New'", mis.getState(), OBSMemArtsCCInputStream.State.NEW);
        int offset = 0;

        int readsize = 256 * 1024;
        int bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertEquals(bytesRead, readsize);
        assertEquals("should in OREAD State", mis.getState(), OBSMemArtsCCInputStream.State.OREAD);

        offset += bytesRead + 512 * 1024;
        fsDataInputStream.seek(offset);
        readsize = 256 * 1024;
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertEquals(bytesRead, readsize);
    }

    protected void runTailRead() throws IOException, NoSuchFieldException, IllegalAccessException {
        Path testFile = setPath("/test/testSmallFileRead.txt");
        int size = 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, testFile, size, 256, 255);
        LOG.info("5MB file created: testMRead2ORead.txt");
        FSDataInputStream fsDataInputStream = this.fs.open(testFile);
        OBSMemArtsCCInputStream mis = (OBSMemArtsCCInputStream) fsDataInputStream.getWrappedStream();

        changeReadAheadRange(1 * 1024 * 1024);
        changeState(mis, OBSMemArtsCCInputStream.State.NEW);
        assertEquals("input state should be 'New'", mis.getState(), OBSMemArtsCCInputStream.State.NEW);
        // Seek to Tail position
        int offset = size - 8 * 1024;
        fsDataInputStream.seek(offset);

        int readsize = 1 * 1024;
        int bytesRead = 0;

        for (int i = 0; i < 8; i++) {
            bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
            offset += bytesRead;
            assertEquals(bytesRead, readsize);
        }

        offset -= 8 * 1024;
        fsDataInputStream.seek(offset);
        for (int i = 0; i < 8; i++) {
            bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
            offset += bytesRead;
            assertEquals(bytesRead, readsize);
        }
    }

    protected void runStatisticsOffLimit() throws IOException, NoSuchFieldException, IllegalAccessException {
        Path testFile = setPath("/test/testStatisticsOffLimit.txt");
        int size = 5 * 1024 * 1024;

        ContractTestUtils.generateTestFile(this.fs, testFile, size, 256, 255);
        LOG.info("5MB file created: testStatisticsOffLimit.txt");
        FSDataInputStream fsDataInputStream = this.fs.open(testFile);
        OBSMemArtsCCInputStream mis = (OBSMemArtsCCInputStream) fsDataInputStream.getWrappedStream();

        // Change statistics to its max limit
        this.fs.getTrafficStatistics().increase(Long.MAX_VALUE, Q);
        this.fs.getTrafficStatistics().increase(Long.MAX_VALUE, Q1);
        this.fs.getTrafficStatistics().increase(Long.MAX_VALUE, Q2);

        changeReadAheadRange(1 * 1024 * 1024);
        changeState(mis, OBSMemArtsCCInputStream.State.NEW);
        assertEquals("input state should be 'New'", mis.getState(), OBSMemArtsCCInputStream.State.NEW);
        int offset = 0;

        int readsize = 1 * 1024 * 1024;
        int bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertEquals(bytesRead, readsize);
        offset += bytesRead;

        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertEquals(bytesRead, readsize);
        offset += bytesRead;

        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertEquals(bytesRead, readsize);
        offset += bytesRead;

        changeState(mis, OBSMemArtsCCInputStream.State.NEW);
        bytesRead = fullReadAndCheck(fsDataInputStream, offset, readsize, false);
        assertEquals(bytesRead, readsize);
        offset += bytesRead;
    }
}
