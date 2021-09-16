package org.apache.flink.fs.obshadoop;

import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.hadoop.fs.FileSystem;

/**
 * @Author:l30002155 @Email:lixianwei6@huawei.com @Date:2020/9/15 9:10
 */
public class FlinkOBSFileSystem extends HadoopFileSystem {

    public FlinkOBSFileSystem(FileSystem obsFileSystem) {
        super(obsFileSystem);
    }

    @Override
    public RecoverableWriter createRecoverableWriter() {
        if (!"obs".equals(getHadoopFileSystem().getScheme())) {
            throw new UnsupportedOperationException(
                "This obs file system "
                    + "implementation does not support recoverable writers.");
        }

        return new FlinkOBSRecoverableWriter(getHadoopFileSystem());
    }

    @Override
    public FileSystemKind getKind() {
        return FileSystemKind.OBJECT_STORE;
    }
}
