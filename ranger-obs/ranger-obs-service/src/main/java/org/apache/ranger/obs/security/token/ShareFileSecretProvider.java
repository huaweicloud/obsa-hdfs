/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.security.token;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.ranger.obs.server.ServerConstants;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;

import javax.crypto.KeyGenerator;
import javax.crypto.spec.SecretKeySpec;

/**
 * utilize distributed storage systems such as hdfs store secret key which
 * need by SimpleSecretManager; if secretkey file don't exist, will Automatic generate
 */
public class ShareFileSecretProvider implements SecretProvider {
    @Override
    public byte[] getSecret(Configuration conf) throws IOException {
        FileSystem fs = null;
        FSDataOutputStream out = null;
        FSDataInputStream in = null;
        try {
            fs = FileSystem.get(conf);
            if (!(fs instanceof DistributedFileSystem)) {
                throw new UnsupportedOperationException(
                    "ranger-obs-service secret manager must be saved on HDFS filesystem");
            }

            KeyStore keyStore = KeyStore.getInstance("jceks");

            Path home = fs.getHomeDirectory();
            String defaultSecret = home.toString() + "/secret.jceks";
            String secretFile = conf.get(ServerConstants.RANGER_OBS_SERVICE_DT_SECRET_FILE, defaultSecret);
            Path path = new Path(secretFile);

            String address = conf.get(ServerConstants.RANGER_OBS_SERVICE_RPC_ADDRESS,
                ServerConstants.DEFAULT_RANGER_OBS_SERVICE_RPC_ADDRESS);
            InetSocketAddress addr = NetUtils.createSocketAddr(address, 26901);
            String hostName = InetAddress.getLocalHost().getHostName() + "_" + addr.getPort();
            Path tmp = new Path(path.toString() + "_" + hostName + "_" + System.currentTimeMillis());
            if (!fs.exists(path)) {
                KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
                keyGenerator.init(128);
                byte[] encoded = keyGenerator.generateKey().getEncoded();

                keyStore.load((InputStream) null, "none".toCharArray());
                keyStore.setKeyEntry("ranger.obs.service.dt.secret", new SecretKeySpec(encoded, "AES"),
                    "none".toCharArray(), (Certificate[]) null);
                out = FileSystem.create(fs, tmp, new FsPermission("600"));
                keyStore.store(out, "none".toCharArray());

                if (!fs.rename(tmp, path)) {
                    fs.delete(tmp, true);
                }
            }

            in = fs.open(path);
            keyStore.load(in, "none".toCharArray());
            Key key = keyStore.getKey("ranger.obs.service.dt.secret", "none".toCharArray());
            return key.getEncoded();
        } catch (NoSuchAlgorithmException | IOException | CertificateException | KeyStoreException | UnrecoverableKeyException e) {
            throw new IOException("get secret failed", e);
        } finally {
            if (out != null) {
                out.close();
            }
            if (in != null) {
                in.close();
            }
            if (fs != null) {
                fs.close();
            }
        }
    }
}
