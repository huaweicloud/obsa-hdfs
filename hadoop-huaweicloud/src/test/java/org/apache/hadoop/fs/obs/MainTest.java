package org.apache.hadoop.fs.obs;

import org.apache.hadoop.fs.obs.contract.TestOBSContractAppend;
import org.apache.hadoop.fs.obs.contract.TestOBSContractCreate;
import org.apache.hadoop.fs.obs.contract.TestOBSContractDelete;
import org.apache.hadoop.fs.obs.contract.TestOBSContractGetFileStatus;
import org.apache.hadoop.fs.obs.contract.TestOBSContractMkdir;
import org.apache.hadoop.fs.obs.contract.TestOBSContractOpen;
import org.apache.hadoop.fs.obs.contract.TestOBSContractRename;
import org.apache.hadoop.fs.obs.contract.TestOBSContractRootDir;
import org.apache.hadoop.fs.obs.contract.TestOBSContractSeek;
import org.apache.hadoop.fs.obs.security.AuthorizeProviderTest;
import org.junit.internal.TextListener;
import org.junit.runner.JUnitCore;

/**
 * Test main class
 */
public class MainTest {
    public static void main(String[] args) {
        if (args.length != 4) {
            throw new IllegalArgumentException("ERROR: parameters invalid, expected 4 args, but get " + args.length);
        }
        // initialise test environment info
        OBSTestUtils.userAk = args[0];
        OBSTestUtils.userSk = args[1];
        OBSTestUtils.endPoint = args[2];
        OBSTestUtils.bucketName = args[3];
        System.out.println("Begin to run tests!");
        JUnitCore engine = new JUnitCore();
        engine.addListener(new TextListener(System.out)); // required to print reports
        System.out.println("Begin to run ITestOBSAppend tests:");
        engine.run(ITestOBSAppend.class);
        System.out.println("Begin to run ITestOBSAppendOutputStream tests:");
        engine.run(ITestOBSAppendOutputStream.class);
        System.out.println("Begin to run ITestOBSBlockingThreadPoolExecutorService tests:");
        engine.run(ITestOBSBlockingThreadPoolExecutorService.class);
        System.out.println("Begin to run ITestOBSBlockOutputArray tests:");
        engine.run(ITestOBSArrayBufferOutputStream.class);
        System.out.println("Begin to run ITestOBSBlockOutputStream tests:");
        engine.run(ITestOBSDiskBufferOutputStream.class);
        System.out.println("Begin to run ITestOBSBlockSize tests:");
        engine.run(ITestOBSBlockSize.class);
        System.out.println("Begin to run ITestOBSBucketAcl tests:");
        engine.run(ITestOBSBucketAcl.class);
        System.out.println("Begin to run ITestOBSCloseAndFinalize tests:");
        engine.run(ITestOBSCloseFunction.class);
        System.out.println("Begin to run ITestOBSConfiguration tests:");
        engine.run(ITestOBSConfiguration.class);
        System.out.println("Begin to run ITestOBSCopyFromLocalFile tests:");
        engine.run(ITestOBSCopyFromLocalFile.class);
        System.out.println("Begin to run ITestOBSCreate tests:");
        engine.run(ITestOBSCreate.class);
        System.out.println("Begin to run ITestOBSCredentialsInURL tests:");
        engine.run(ITestOBSCredentialsInURL.class);
        System.out.println("Begin to run ITestOBSDataBlocks tests:");
        engine.run(ITestOBSDataBlocks.class);
        System.out.println("Begin to run ITestOBSDefaultInformation tests:");
        engine.run(ITestOBSDefaultInformation.class);
        System.out.println("Begin to run ITestOBSDeleteAndRenameManyFiles tests:");
        engine.run(ITestOBSDeleteAndRenameManyFiles.class);
        System.out.println("Begin to run ITestOBSFileSystem tests:");
        engine.run(ITestOBSFileSystem.class);
        System.out.println("Begin to run ITestOBSFileSystemContract tests:");
        engine.run(ITestOBSFileSystemContract.class);
        System.out.println("Begin to run ITestOBSFSDataOutputStream tests:");
        engine.run(ITestOBSFSDataOutputStream.class);
        System.out.println("Begin to run ITestOBSGetAndSetWorkingDirectory tests:");
        engine.run(ITestOBSGetAndSetWorkingDirectory.class);
        System.out.println("Begin to run ITestOBSGetContentSummary tests:");
        engine.run(ITestOBSGetContentSummary.class);
        System.out.println("Begin to run ITestOBSGetFileStatusAndExist tests:");
        engine.run(ITestOBSGetFileStatusAndExist.class);
        System.out.println("Begin to run ITestOBSHFlush tests:");
        engine.run(ITestOBSHFlush.class);
        System.out.println("Begin to run ITestOBSInputStream tests:");
        engine.run(ITestOBSInputStream.class);
        System.out.println("Begin to run ITestOBSMemArtsCCInputStream tests:");
        engine.run(ITestOBSMemArtsCCInputStream.class);
        System.out.println("Begin to run ITestOBSMemArtsCCInputStreamBufIO tests:");
        engine.run(ITestOBSMemArtsCCInputStreamBufIO.class);
        System.out.println("Begin to run ITestOBSTruncate tests:");
        engine.run(ITestOBSTruncate.class);
        System.out.println("Begin to run ITestOBSListFiles tests:");
        engine.run(ITestOBSListFiles.class);
        System.out.println("Begin to run ITestOBSListLocatedStatus tests:");
        engine.run(ITestOBSListLocatedStatus.class);
        System.out.println("Begin to run ITestOBSListStatus tests:");
        engine.run(ITestOBSListStatus.class);
        System.out.println("Begin to run ITestOBSMetricInfo tests:");
        engine.run(ITestOBSMetricInfo.class);
        System.out.println("Begin to run ITestOBSMkdirs tests:");
        engine.run(ITestOBSMkdirs.class);
        System.out.println("Begin to run ITestOBSMultiDeleteObjects tests:");
        engine.run(ITestOBSMultiDeleteObjects.class);
        System.out.println("Begin to run ITestOBSOpen tests:");
        engine.run(ITestOBSOpen.class);
        System.out.println("Begin to run ITestOBSWriteOperationHelper tests:");
        engine.run(ITestOBSWriteOperationHelper.class);
        System.out.println("Begin to run ITestOBSOutputStream tests:");
        engine.run(ITestOBSOutputStream.class);
        System.out.println("Begin to run ITestOBSRename tests:");
        engine.run(ITestOBSRename.class);
        System.out.println("Begin to run ITestOBSRetryMechanism tests:");
        engine.run(ITestOBSRetryMechanism.class);
        System.out.println("Begin to run ITestOBSRetryMechanism2 tests:");
        engine.run(ITestOBSRetryMechanism2.class);
        System.out.println("Begin to run ITestOBSInvoker tests:");
        engine.run(ITestOBSInvoker.class);
        System.out.println("Begin to run ITestOBSBucketPolicy tests:");
        engine.run(ITestOBSBucketPolicy.class);
        System.out.println("Begin to run ITestOBSCloseCheck tests:");
        engine.run(ITestOBSCloseProtect.class);
        System.out.println("Begin to run ITestOBSTrash tests:");
        engine.run(ITestOBSFastDelete.class);
        System.out.println("Begin to run ITestOBSTestUtils tests:");
        engine.run(ITestOBSTestUtils.class);
        System.out.println("Begin to run TestOBSContractAppend tests:");
        engine.run(TestOBSContractAppend.class);
        System.out.println("Begin to run TestOBSContractCreate tests:");
        engine.run(TestOBSContractCreate.class);
        System.out.println("Begin to run TestOBSContractDelete tests:");
        engine.run(TestOBSContractDelete.class);
        System.out.println("Begin to run TestOBSContractGetFileStatus tests:");
        engine.run(TestOBSContractGetFileStatus.class);
        System.out.println("Begin to run TestOBSContractMkdir tests:");
        engine.run(TestOBSContractMkdir.class);
        System.out.println("Begin to run TestOBSContractOpen tests:");
        engine.run(TestOBSContractOpen.class);
        System.out.println("Begin to run TestOBSContractRename tests:");
        engine.run(TestOBSContractRename.class);
        System.out.println("Begin to run TestOBSContractRootDir tests:");
        engine.run(TestOBSContractRootDir.class);
        System.out.println("Begin to run TestOBSContractSeek tests:");
        engine.run(TestOBSContractSeek.class);
        System.out.println("Begin to run TestOBSFileContextCreateMkdir tests:");
        engine.run(TestOBSFileContextCreateMkdir.class);
        System.out.println("Begin to run TestOBSFileContextMainOperations tests:");
        engine.run(TestOBSFileContextMainOperations.class);
        System.out.println("Begin to run TestOBSFileContextURI tests:");
        engine.run(TestOBSFileContextURI.class);
        System.out.println("Begin to run TestOBSFileContextUtil tests:");
        engine.run(TestOBSFileContextUtil.class);
        System.out.println("Begin to run TestOBSFileSystemContract tests:");
        engine.run(TestOBSFileSystemContract.class);
        System.out.println("Begin to run TestOBSFSMainOperations tests:");
        engine.run(TestOBSFSMainOperations.class);
        System.out.println("Begin to run ITestOBSHDFSWrapperFileSystem tests:");
        engine.run(ITestOBSHDFSWrapperFileSystem.class);
        System.out.println("Begin to run ITestOBSHDFSWrapper tests:");
        engine.run(ITestOBSHDFSWrapper.class);
        System.out.println("Begin to run ITestOBSLoginHelper tests:");
        engine.run(ITestOBSLoginHelper.class);
        System.out.println("Begin to run AuthorizeProviderTest tests:");
        engine.run(AuthorizeProviderTest.class);
        System.out.println("Begin to run ITestOBSMemArtsCCInputStreamTrafficReport tests:");
        engine.run(ITestOBSMemArtsCCInputStreamTrafficReport.class);
        System.out.println("Begin to run ITestOBSMemArtsCCInputStreamStatistics tests:");
        engine.run(ITestOBSMemArtsCCInputStreamStatistics.class);
        System.out.println("Begin to run ITestOBSFastDeleteV2 tests:");
        engine.run(ITestOBSFastDeleteV2.class);
        System.out.println("Begin to run ITestOBSDisguisePermissionSupport tests:");
        engine.run(ITestOBSDisguisePermissionSupport.class);
        System.out.println("Running finished.");
    }
}
