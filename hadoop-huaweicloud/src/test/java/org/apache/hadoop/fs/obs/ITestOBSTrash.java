package org.apache.hadoop.fs.obs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ITestOBSTrash {
    private OBSFileSystem fs;
    private static String testRootPath =
            OBSTestUtils.generateUniqueTestPath();
    @Before
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean("fs.obs.trash.enable",true);
        conf.set("fs.obs.trash.dir","Trash/");
        fs = OBSTestUtils.createTestFileSystem(conf);
    }
    private Path getTestPath(){return new Path(testRootPath + "/test-delet/1"); }
    @Test
    public void deleFolderToTrash() throws IOException {
        final byte[] data = ContractTestUtils.dataset(1024, 'a', 'z');
        List<Path> pathList = new ArrayList<Path>();
        for(int i = 0 ; i < 3; i++) {
            String objectName = "objectINfolder-"+i;
            Path objectPath = new Path(getTestPath(), objectName);
            ContractTestUtils.createFile(fs, objectPath, false, data);
            pathList.add(objectPath);
        }
        ListObjectsRequest request = new ListObjectsRequest(fs.getBucket());
        request.setPrefix("test/");
        ObjectListing result = fs.getObsClient().listObjects(request);
        for (ObsObject obsObject : result.getObjects()){
            System.out.println("\t" + obsObject.getObjectKey());
        }
        fs.delete(getTestPath(),true);
//        fs.delete(new Path("/trash/1"),true);
    }
    @Test
    public void deleObjectToTrash() throws IOException {
        final byte[] data = ContractTestUtils.dataset(1024,'a', 'z');
        Path objectPath = new Path(testRootPath + "/test-delet/object");
        ContractTestUtils.createFile(fs, objectPath, false, data);
        fs.delete(objectPath,true);
//        fs.delete(new Path("/trash/object"),true);
    }

    @Test
    public void testNotSupport() throws IOException {
        final byte[] data = ContractTestUtils.dataset(1024,'a', 'z');
        Path objectPath = new Path(testRootPath + "/test-delet/object");
        ContractTestUtils.createFile(fs, objectPath, false, data);
        String exception = "";
        try {
            fs.delete(objectPath,true);
        } catch (IOException e) {
            exception = e.getMessage();
        }
        assertTrue(exception.contains("not supported for trash."));
    }
}
