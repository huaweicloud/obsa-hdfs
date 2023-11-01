package org.apache.hadoop.fs.obs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.obs.services.exception.ObsException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(MockitoJUnitRunner.class)
public class ITestOBSInvoker {
    private static final int COMMON_RETRY_LIMIT = 5;

    private static final int QOS_RETRY_LIMIT = 7;

    private int retryCount;

    private OBSInvoker invoker;
    private OBSInvoker invoker_fail;
    private OBSFileSystem fs;

    @Rule
    public OBSTestRule testRule = new OBSTestRule();


    @Before
    public void setup() {
        retryCount = 0;
        Configuration conf = new Configuration();
        conf.setInt(OBSConstants.RETRY_LIMIT, COMMON_RETRY_LIMIT);
        conf.setInt(OBSConstants.RETRY_QOS_LIMIT, QOS_RETRY_LIMIT);

        conf.setLong(OBSConstants.RETRY_SLEEP_BASETIME, 5);
        conf.setLong(OBSConstants.RETRY_SLEEP_MAXTIME, 10);
        conf.setLong(OBSConstants.RETRY_QOS_SLEEP_BASETIME, 5);
        conf.setLong(OBSConstants.RETRY_QOS_SLEEP_MAXTIME, 10);

        try {
            fs = OBSTestUtils.createTestFileSystem(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        OBSRetryPolicy RETRY_POLICY = new OBSRetryPolicy(conf);
        invoker = new OBSInvoker(fs, RETRY_POLICY,  (text, e, retries, idempotent) -> retryCount++);

        conf.setLong(OBSConstants.RETRY_MAXTIME, 10);
        conf.setLong(OBSConstants.RETRY_QOS_MAXTIME, 10);
        OBSRetryPolicy RETRY_POLICY1 = new OBSRetryPolicy(conf);
        invoker_fail = new OBSInvoker(fs, RETRY_POLICY1,  (text, e, retries, idempotent) -> retryCount++);
    }

    @Test
    public void testNonIOException() {
        //can not process non IOException
    }

    @Test
    public void testAccessControlException() throws Exception {
        ObsException obsException = new ObsException("test");
        obsException.setResponseCode(403);
        try {
            invoker.retryByMaxTime(OBSOperateAction.write,"test",
                () -> {
                    throw obsException;
                }, true);
        } catch (AccessControlException e) {
            Assert.assertEquals("AccessControlException",0,retryCount);
        }
    }

    @Test
    public void testFileNotFoundException() throws Exception {
        ObsException obsException = new ObsException("test");
        obsException.setResponseCode(404);
        try {
            invoker.retryByMaxTime(OBSOperateAction.write,"test",
                () -> {
                    throw obsException;
                }, true);
        } catch (FileNotFoundException e) {
            Assert.assertEquals("FileNotFoundException",0,retryCount);
        }
    }

    @Test
    public void testOBSFileConflictException() throws Exception {
        ObsException obsException = new ObsException("test");
        obsException.setResponseCode(409);
        try {
            invoker.retryByMaxTime(OBSOperateAction.write,"test",
                () -> {
                    throw obsException;
                }, true);
        } catch (OBSFileConflictException e) {
            Assert.assertEquals("OBSFileConflictException",0,retryCount);
        }
    }

    @Test
    public void testOBSIllegalArgumentException() throws Exception {
        ObsException obsException = new ObsException("test");
        obsException.setResponseCode(400);
        try {
            invoker.retryByMaxTime(OBSOperateAction.write,"test",
                () -> {
                    throw obsException;
                }, true);
        } catch (OBSIllegalArgumentException e) {
            Assert.assertEquals("OBSIllegalArgumentException",0,retryCount);
        }
    }

    @Test(expected = OBSIOException.class)
    public void testOBSIOException() throws Exception {
        ObsException obsException = new ObsException("test");
        obsException.setResponseCode(500);
        invoker_fail.retryByMaxTime(OBSOperateAction.write,"test",
            () -> {
                throw obsException;
            }, true);

    }

    @Test
    public void testOBSIOExceptionSuccess() throws Exception {
        ObsException obsException = new ObsException("test");
        obsException.setResponseCode(500);
        final AtomicInteger counter = new AtomicInteger(0);
        invoker.retryByMaxTime(OBSOperateAction.write,"test",
            () -> {
                if (counter.incrementAndGet() < COMMON_RETRY_LIMIT) {
                    throw obsException;
                }
                return null;
            }, true);
        assertEquals(COMMON_RETRY_LIMIT-1, retryCount);
    }

    @Test(expected = SocketTimeoutException.class)
    public void testIOException() throws IOException {
        ObsException obsException = new ObsException("test");
        obsException.setResponseCode(500);
        invoker_fail.retryByMaxTime(OBSOperateAction.write,"test",
            () -> {
                throw new SocketTimeoutException("test");
            }, true);
    }

    @Test
    public void testIOExceptionSuccess() throws IOException {
        final AtomicInteger counter = new AtomicInteger(0);
        invoker.retryByMaxTime(OBSOperateAction.write,"test",
            () -> {
                if (counter.incrementAndGet() < QOS_RETRY_LIMIT) {
                    throw new SocketTimeoutException("test");
                }
                return null;
            }, true);
        assertEquals( QOS_RETRY_LIMIT-1, retryCount);
    }

    @Test(expected = OBSQosException.class)
    public void testOBSQosException() throws Exception {
        ObsException obsException = new ObsException("test");
        obsException.setResponseCode(503);
        obsException.setErrorCode(OBSCommonUtils.DETAIL_QOS_CODE);
        invoker_fail.retryByMaxTime(OBSOperateAction.write,"test",
            () -> {
                throw obsException;
            }, true);

    }

    @Test
    public void testOBSQosExceptionSuccess() throws Exception {
        ObsException obsException = new ObsException("test");
        obsException.setResponseCode(503);
        obsException.setErrorCode(OBSCommonUtils.DETAIL_QOS_CODE);
        final AtomicInteger counter = new AtomicInteger(0);
        invoker.retryByMaxTime(OBSOperateAction.write,"test",
            () -> {
                if (counter.incrementAndGet() < QOS_RETRY_LIMIT) {
                    throw obsException;
                }
                return null;
            }, true);
        assertEquals(QOS_RETRY_LIMIT-1, retryCount);
    }

    //translate exception
    @Test(expected = OBSQosException.class)
    public void testTranslateOBSException() throws Exception {
        ObsException obsException = new ObsException("test");
        obsException.setResponseCode(503);
        OBSQosException qosException = new OBSQosException("test", obsException);
        obsException.setErrorCode(OBSCommonUtils.DETAIL_QOS_CODE);
        invoker_fail.retryByMaxTime(OBSOperateAction.write,"test",
            () -> {
                throw qosException;
            }, true);
    }

    //translate exception
    @Test
    public void testTranslateOBSExceptionSuccess() throws Exception {
        ObsException obsException = new ObsException("test");
        obsException.setResponseCode(503);
        OBSQosException qosException = new OBSQosException("test", obsException);
        obsException.setErrorCode(OBSCommonUtils.DETAIL_QOS_CODE);
        final AtomicInteger counter = new AtomicInteger(0);
        invoker.retryByMaxTime(OBSOperateAction.write,"test",
            () -> {
                if (counter.incrementAndGet() < QOS_RETRY_LIMIT) {
                    throw qosException;
                }
                return null;
            }, true);
        assertEquals(QOS_RETRY_LIMIT-1, retryCount);
    }

    //nonIdempotent exception
    @Test(expected = OBSIOException.class)
    public void testNonIdempotentOBSException() throws Exception {
        ObsException obsException = new ObsException("test");
        obsException.setResponseCode(500);
        invoker.retryByMaxTime(OBSOperateAction.write,"test",
            () -> {
                throw obsException;
            }, false);
    }
}