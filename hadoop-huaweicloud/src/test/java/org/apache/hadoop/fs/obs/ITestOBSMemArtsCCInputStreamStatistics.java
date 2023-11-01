package org.apache.hadoop.fs.obs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ITestOBSMemArtsCCInputStreamStatistics extends ITestOBSMemArtsCCInputStreamStatisticsTestBase {

    @Test
    public void testNew2ORead2MRead2ORead() throws Exception {
        runNew2ORead2MRead2ORead();
        long bytesRead = this.fs.getSchemeStatistics().getBytesRead();
        assertEquals(3145728, bytesRead);

        tearDownFS();
    }

    @Test
    public void testLazySeek2MRead() throws Exception {
        runLazySeek2MRead();
        long bytesRead = this.fs.getSchemeStatistics().getBytesRead();
        assertEquals(1049088, bytesRead);

        tearDownFS();
    }

    @Test
    public void testBackSeekAndRead() throws Exception {
        runBackSeekAndRead();
        long bytesRead = this.fs.getSchemeStatistics().getBytesRead();
        assertEquals(1048576, bytesRead);

        tearDownFS();
    }

    @Test
    public void testSeekAndRead() throws Exception {
        runSeekAndRead();
        long bytesRead = this.fs.getSchemeStatistics().getBytesRead();
        assertEquals(1048576, bytesRead);

        tearDownFS();
    }
}
