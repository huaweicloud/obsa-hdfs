package org.apache.hadoop.fs.obs.memartscc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.obs.OBSFileSystem;
import org.apache.hadoop.fs.obs.security.DelegationTokenProvider;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class MemArtsCCDelegationTokenProvider implements DelegationTokenProvider {
    private static final Logger logger = LoggerFactory.getLogger(MemArtsCCDelegationTokenProvider.class);

    public static final String ServiceNameKey = "fs.obs.memartscc.service.name";

    public static final String DefaultServiceName = "memartscc";

    public static String getCanonicalName(Configuration conf) {
        return conf.get(ServiceNameKey, DefaultServiceName);
    }

    private String canonicalServiceName;

    private Configuration conf;

    private URI uri;

    private OBSFileSystem fs;

    @Override
    public void initialize(FileSystem fs, URI uri, Configuration conf) {
        this.canonicalServiceName = getCanonicalName(conf);
        this.uri = uri;
        this.conf = conf;
        if (fs instanceof OBSFileSystem) {
            this.fs = (OBSFileSystem) fs;
        } else {
            throw new IllegalArgumentException("fs only support OBSFileSystem");
        }
    }

    @Override
    public Token<?> getDelegationToken(String renewer) {
        logger.info("get delegation token for renewer {}", renewer);
        try {
            MemArtsCCDelegationTokenIdentifier tokenIdentifier = new MemArtsCCDelegationTokenIdentifier(
                new Text(renewer));
            byte[] password = fs.getMemArtsCCClient().getDT();
            return new Token<MemArtsCCDelegationTokenIdentifier>(tokenIdentifier.getBytes(), password,
                MemArtsCCDelegationTokenIdentifier.MEMARTSCC_DELEGATION_KIND, new Text(this.canonicalServiceName));
        } catch (Exception e) {
            logger.warn("get dt from memartscc failed", e);
        }
        return null;
    }

    @Override
    public String getCanonicalServiceName() {
        return canonicalServiceName;
    }
}
