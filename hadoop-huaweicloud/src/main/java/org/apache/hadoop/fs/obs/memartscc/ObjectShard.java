package org.apache.hadoop.fs.obs.memartscc;

/**
 * 功能描述
 *
 * @since 2021-05-24
 */
public class ObjectShard {
    long start;

    long end;

    String[] hosts;

    int validHostNum;

    public ObjectShard(long start, long end, String[] hosts, int validHostNum) {
        this.start = start;
        this.end = end;
        this.hosts = hosts;
        this.validHostNum = validHostNum;
    }

    public ObjectShard(String[] hosts) {
        this.hosts = hosts;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public String[] getHosts() {
        return hosts;
    }

    public int getValidHostNum() {
        return validHostNum;
    }
}
