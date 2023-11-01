package org.apache.hadoop.fs.obs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import com.obs.services.model.PutObjectRequest;
import com.obs.services.model.UploadPartRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.obs.contract.OBSContract;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class ITestOBSWriteOperationHelper {

    private static OBSWriteOperationHelper writeHelper;

    @BeforeClass
    public static void beforeClass() throws IOException {
        Configuration conf = OBSContract.getConfiguration(null);
        OBSFileSystem fileSystem = OBSTestUtils.createTestFileSystem(conf);
        writeHelper = new OBSWriteOperationHelper(fileSystem);
    }

    @Test
    public void testNewPutRequestForChecksumType() {
        final String digest = "123";
        byte[] data = new byte[10];
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
        PutObjectRequest request =
            writeHelper.newPutRequest("object01", inputStream, data.length, OBSDataBlocks.ChecksumType.NONE, digest);
        assertNull(request.getMetadata().getContentMd5());
        assertFalse(request.getUserHeaders().containsKey(OBSWriteOperationHelper.CONTENT_SHA256));

        request = writeHelper.newPutRequest("object01", inputStream, data.length, OBSDataBlocks.ChecksumType.MD5, digest);
        assertEquals(digest, request.getMetadata().getContentMd5());
        assertFalse(request.getUserHeaders().containsKey(OBSWriteOperationHelper.CONTENT_SHA256));

        request =
            writeHelper.newPutRequest("object01", inputStream, data.length, OBSDataBlocks.ChecksumType.SHA256, digest);
        assertNull(request.getMetadata().getContentMd5());
        assertEquals(digest, request.getUserHeaders().get(OBSWriteOperationHelper.CONTENT_SHA256));
    }

    @Test
    public void testNewUploadPartRequestForChecksumType() {
        final String digest = "123";
        byte[] data = new byte[10];
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
        UploadPartRequest request =
            writeHelper.newUploadPartRequest("object01", "1", 1, 10, inputStream, OBSDataBlocks.ChecksumType.NONE, digest);
        assertNull(request.getContentMd5());
        assertFalse(request.getUserHeaders().containsKey(OBSWriteOperationHelper.CONTENT_SHA256));

        request =
            writeHelper.newUploadPartRequest("object01", "1", 1, 10, inputStream, OBSDataBlocks.ChecksumType.MD5, digest);
        assertEquals(digest, request.getContentMd5());
        assertFalse(request.getUserHeaders().containsKey(OBSWriteOperationHelper.CONTENT_SHA256));

        request =
            writeHelper.newUploadPartRequest("object01", "1", 1, 10, inputStream, OBSDataBlocks.ChecksumType.SHA256, digest);
        assertNull(request.getContentMd5());
        assertEquals(digest, request.getUserHeaders().get(OBSWriteOperationHelper.CONTENT_SHA256));
    }

}
