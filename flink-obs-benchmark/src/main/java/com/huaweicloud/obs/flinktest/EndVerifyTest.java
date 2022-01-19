package com.huaweicloud.obs.flinktest;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.TreeMap;

/**
 * 功能描述
 *
 * @since 2021-05-17
 */
public class EndVerifyTest {
    public static final Logger LOG = LoggerFactory.getLogger(EndVerifyTest.class);

    public static void main(String[] args) throws URISyntaxException, IOException {

        final ParameterTool params = ParameterTool.fromArgs(args);
        String sinkPathStr = params.getRequired("sinkPath");
        String totalRecords = params.getRequired("totalRecords");

        URI uri = new URI(sinkPathStr);
        Path sinkPath = new Path(uri);

        FileSystem fileSystem = FileSystem.get(uri);

        FileStatus[] fileStatuses = fileSystem.listStatus(sinkPath);
        TreeMap<String, Path> treeMap = new TreeMap<>();
        for (FileStatus fileStatus : fileStatuses) {
            if (!fileStatus.isDir()) {
                Path path = fileStatus.getPath();
                String fileName = path.getName();
                String substring = fileName.substring(fileName.lastIndexOf("-") + 1, fileName.length());
                treeMap.put(substring, path);
            }
        }

        long count = 1;
        boolean error = false;
        if (treeMap.size() == 0) {
            error = true;
        }
        for (Map.Entry<String, Path> entry : treeMap.entrySet()) {

            Path path = entry.getValue();

            BufferedReader tread = null;
            FSDataInputStream open = null;
            try {
                open = fileSystem.open(path);
                tread = new BufferedReader(new InputStreamReader(open, "UTF-8"));
                String tline = null;
                while ((tline = tread.readLine()) != null) {
                    int i = Integer.parseInt(tline.trim());
                    if (!(i == count)) {
                        error = true;
                        break;
                    }
                    count++;
                }
            } catch (IOException e) {
                //这个主要是把出现的异常给人看见，不然就算异常了，看不到就找不到问题所在。
                LOG.debug("loadProperties IOException:" + e.getMessage());
            } finally {
                if (open != null) {
                    open.close();
                }
                if (tread != null) {
                    tread.close();
                }
            }

            if (error) {
                break;
            }
        }
        if (Long.parseLong(totalRecords) + 1 != count) {
            error = true;
        }
        if (error) {
            System.out.println("verify failed");
        } else {
            System.out.println("verify success");
        }
    }

}
