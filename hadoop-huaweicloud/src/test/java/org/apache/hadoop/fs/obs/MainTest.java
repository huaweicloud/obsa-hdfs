package org.apache.hadoop.fs.obs;

import org.junit.internal.TextListener;
import org.junit.runner.JUnitCore;

/**
 * Test main class
 */
public class MainTest {
    public static void main(String[] args) {
        System.out.println("Running tests!");
        JUnitCore engine = new JUnitCore();
        engine.addListener(new TextListener(System.out)); // required to print reports
        engine.run(ITestOBSInputStream.class);
        System.out.println("Running finished.");

    }
}
