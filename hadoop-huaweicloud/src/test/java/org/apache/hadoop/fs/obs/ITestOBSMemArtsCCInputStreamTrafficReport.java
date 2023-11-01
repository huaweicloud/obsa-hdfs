package org.apache.hadoop.fs.obs;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ITestOBSMemArtsCCInputStreamTrafficReport extends ITestOBSMemArtsCCInputStreamStatisticsTestBase {

    @Test
    public void testNew2ORead2MRead2ORead() throws Exception {
        runNew2ORead2MRead2ORead();

        tearDownFS();
        mockMemArtsCCClient.printTotalStatistics();
        String[] outputArray = outContent.toString().split("\n");
        assertEquals("Total: Q:3145728 Q`:3670015 Q2:1572863 Q1:2097152",
                outputArray[outputArray.length - 1]);
    }

    @Test
    public void testLazySeek2MRead() throws Exception {
        runLazySeek2MRead();

        tearDownFS();
        mockMemArtsCCClient.printTotalStatistics();
        String[] outputArray = outContent.toString().split("\n");
        assertEquals("Total: Q:2097152 Q`:1573120 Q2:524544 Q1:1048576",
                outputArray[outputArray.length - 1]);
    }

    @Test
    public void testBackSeekAndRead() throws Exception {
        runBackSeekAndRead();

        tearDownFS();
        mockMemArtsCCClient.printTotalStatistics();
        String[] outputArray = outContent.toString().split("\n");
        assertEquals("Total: Q:2097152 Q`:1572864 Q2:524288 Q1:1048576",
                outputArray[outputArray.length - 1]);
    }

    @Test
    public void testSeekAndRead() throws Exception {
        runSeekAndRead();

        tearDownFS();
        mockMemArtsCCClient.printTotalStatistics();
        String[] outputArray = outContent.toString().split("\n");
        assertEquals("Total: Q:1048576 Q`:1048576 Q2:0 Q1:1048576",
                outputArray[outputArray.length - 1]);
    }

    @Test
    public void testTailRead() throws Exception {
        runTailRead();

        tearDownFS();
        mockMemArtsCCClient.printTotalStatistics();
        String[] outputArray = outContent.toString().split("\n");
        assertEquals("Total: Q:8192 Q`:8192 Q2:0 Q1:8192",
                outputArray[outputArray.length - 1]);
    }

    @Test
    public void testStatisticsOffLimit() throws Exception {
        runStatisticsOffLimit();

        tearDownFS();
        mockMemArtsCCClient.printTotalStatistics();
        String[] outputArray = outContent.toString().split("\n");
        assertEquals("Total: Q:3145728 Q`:2097152 Q2:1048576 Q1:1048576",
                outputArray[outputArray.length - 1]);
    }

}
