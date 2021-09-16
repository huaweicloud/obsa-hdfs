package org.apache.flink.fs.obshadoop;

import org.junit.internal.TextListener;
import org.junit.runner.JUnitCore;

/**
 * Test main class
 */
public class MainTest {
    public static void main(String[] args) {
        if (args.length != 4) {
            throw new IllegalArgumentException(
                "ERROR: parameters invalid, expected 4 args, but get "
                    + args.length);
        }
        OBSTestCredentials.OBS_TEST_ENDPOINT = args[2];
        OBSTestCredentials.OBS_TEST_ACCESS_KEY = args[0];
        OBSTestCredentials.OBS_TEST_SECRET_KEY = args[1];
        OBSTestCredentials.OBS_TEST_BUCKET = args[3];

        System.out.println("Begin to run tests!");
        JUnitCore engine = new JUnitCore();
        engine.addListener(new TextListener(System.out)); // required to print reports
        System.out.println("Begin to run HadoopOBSFileSystemBehaviorITCase:");
        engine.run(HadoopOBSFileSystemBehaviorITCase.class);
        System.out.println("Begin to run HadoopOBSFileSystemITCase:");
        engine.run(HadoopOBSFileSystemITCase.class);
        System.out.println("Begin to run HadoopOBSFileSystemTest:");
        engine.run(HadoopOBSFileSystemTest.class);
        System.out.println("Begin to run HadoopOBSRecoverableWriterTest:");
        engine.run(HadoopOBSRecoverableWriterTest.class);

        System.out.println("Running finished.");
    }
}
