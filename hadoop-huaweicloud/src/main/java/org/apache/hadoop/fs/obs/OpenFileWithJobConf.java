package org.apache.hadoop.fs.obs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

public interface OpenFileWithJobConf {
    FSDataInputStream open(Path f, Configuration jobConf)
            throws IOException;
}
