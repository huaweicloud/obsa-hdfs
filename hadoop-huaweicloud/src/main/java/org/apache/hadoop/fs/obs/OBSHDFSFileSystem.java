/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.obs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class OBSHDFSFileSystem extends DistributedFileSystem {
    public static final Log LOG = LogFactory.getLog(OBSHDFSFileSystem.class);

    private final static String CONFIG_HDFS_PREFIX = "fs.hdfs.mounttable";

    private final static String CONFIG_HDFS_DEFAULT_MOUNT_TABLE = "default";

    private final static String CONFIG_HDFS_LINK = "link";

    public static final String WRAPPERFS_RESERVED = "/.wrapperfs_reserved";

    private Configuration wrapperConf;

    private Map<String, MountInfo> mountMap;    // key is mount point, should not tail with "/"

    private DistributedFileSystem underHDFS;

    @Override
    public void setWorkingDirectory(Path path) {
        TransferedPath newPath = transferToNewPath(path);

        try {
            newPath.getFS().setWorkingDirectory(newPath.toPath());
        } catch (IOException e) {
            LOG.error("failed to set working directoy");
        }
    }

    private List<Pair<String, String>> getMountList(final Configuration config, final String viewName) {
        List<Pair<String, String>> mountList = new ArrayList<>();
        String mountTableName = viewName;
        if (mountTableName == null) {
            mountTableName = CONFIG_HDFS_DEFAULT_MOUNT_TABLE;
        }
        final String mountTablePrefix = CONFIG_HDFS_PREFIX + "." + mountTableName + ".";
        final String linkPrefix = CONFIG_HDFS_LINK + ".";
        for (Map.Entry<String, String> si : config) {
            final String key = si.getKey();
            if (key.startsWith(mountTablePrefix)) {
                String src = key.substring(mountTablePrefix.length());
                if (src.startsWith(linkPrefix)) {
                    String pathPrefix = src.substring(linkPrefix.length()).trim();
                    if (pathPrefix.endsWith(Path.SEPARATOR) && !pathPrefix.trim().equals(Path.SEPARATOR)) {
                        pathPrefix = pathPrefix.substring(0, pathPrefix.length() - 1);
                    }
                    mountList.add(new Pair<>(pathPrefix, si.getValue()));
                }
            }
        }
        return mountList;
    }

    private List<Pair<String, String>> initMountList(List<Pair<String, String>> mountList) {
        List<Pair<String, String>> mappingPaths = new ArrayList<>();

        // make shorter mount point first, it helps to check subdirectory mount
        Collections.sort(mountList, Comparator.comparingInt(p -> p.getKey().length()));

        for (int i = 0; i < mountList.size(); i++) {
            String mountTarget = mountList.get(i).getValue();
            String mountPoint = mountList.get(i).getKey();
            URI.create(mountTarget);  // just check path format

            boolean conflict = mappingPaths.stream().map(Pair::getKey)
                // if mount point is same with previous, ignore current one
                // if mount point is subdirectory of another one, ignore subdirectory
                .filter(mp -> mountPoint.equals(mp) || mountPoint.startsWith(mp) && mountPoint.substring(mp.length()).startsWith(Path.SEPARATOR))
                .map(mp -> {
                    LOG.warn("mount point: " + mountPoint + " is ignored by shorted mount point: " + mp);
                    return mp;
                })      // just log print
                .findFirst().isPresent();
            if (!conflict) {
                mappingPaths.add(mountList.get(i));
                LOG.info(mountList.get(i).getKey() + "->" + mountList.get(i).getValue());
            }
        }
        return mappingPaths;
    }

    //此方法会被反射调用，维护时候请注意不要影响到功能
    private TransferedPath transferToNewPath(Path path) {
        String inputPath = path.toUri().getPath();

        if (inputPath.startsWith(WRAPPERFS_RESERVED)) {
            // reserved path rule: /.wrapperfs_reserved/<schema>/<authority>/paths
            String[] pathComps = inputPath.split("/", 5);
            if (pathComps.length >= 4) {
                String schema = pathComps[2];
                String authority = pathComps[3];
                if ("null".equals(authority)) {
                    authority = null;
                }
                String reservedPath = "/" + pathComps[4];

                MountInfo mi = findMountWithSchemaAndAuthority(schema, authority);
                if (mi != null) {
                    return new TransferedPath(mi, reservedPath, true);
                } else {
                    MountInfo reservedMountInfo = new MountInfo(
                        String.format("%s/%s/%s/", WRAPPERFS_RESERVED, schema, authority), "/", () -> underHDFS);
                    return new TransferedPath(reservedMountInfo, reservedPath, true);
                }
            }
        }
        //String mountKey : mountMap.keySet()
        for (Map.Entry<String, MountInfo> entry : mountMap.entrySet()) {
            if (inputPath.startsWith(entry.getKey())) {
                String subPath = inputPath.substring(entry.getKey().length());
                // exact match the mount key or followed by another path component
                if (subPath.length() == 0 || subPath.startsWith("/")) {
                    return new TransferedPath(mountMap.get(entry.getKey()), subPath);
                }
            }
        }
        MountInfo originMount = new MountInfo("/", "/", () -> underHDFS);
        return new TransferedPath(originMount, inputPath, false);
    }

    private MountInfo findMountWithSchemaAndAuthority(String schema, String authority) {
        for (MountInfo m : mountMap.values()) {
            try {
                if (Objects.equals(m.getToFileSystem().getUri().getScheme(), schema) && Objects.equals(
                    m.getToFileSystem().getUri().getAuthority(), authority)) {
                    return m;
                }
            } catch (IOException e) {
                LOG.warn("ignore this warn", e);
            }
        }
        return null;
    }

    /**
     * convert in mount path back to wrapped fs path
     *
     * @param inMountPath  the path under the mount point
     * @param mountPointFS
     * @return the path present in default fs
     */
    private Path toWrappedPath(Path inMountPath, FileSystem mountPointFS) throws IOException {
        URI inMountURI = inMountPath.toUri();
        String schema = Optional.ofNullable(inMountURI.getScheme()).orElse(mountPointFS.getScheme());
        String authority = inMountURI.getAuthority();
        String path = inMountURI.getPath();

        for (MountInfo m : mountMap.values()) {
            if (inSameFileSystem(mountPointFS, schema, authority, m) && path.startsWith(m.toPath)) {
                // this inMountPath is sub path under the mount point
                String subFolder = path.substring(m.toPath.length());
                return this.makeQualified(new Path(new Path(m.fromPath), new Path(subFolder)));
            }
        }

        // use reserved path for non-mount path under some mount file system
        return Path.mergePaths(getReservedPath(schema, authority), new Path(path));
    }

    private Path getReservedPath(String schema, String authority) {
        return new Path(String.format(WRAPPERFS_RESERVED + "/%s/%s/", schema, authority));
    }

    private boolean inSameFileSystem(FileSystem mountPointFS, String schema, String authority, MountInfo m)
        throws IOException {
        return m.getToFileSystem() == mountPointFS
            || Objects.equals(m.getToFileSystem().getUri().getScheme(), schema) && Objects.equals(
            m.getToFileSystem().getUri().getAuthority(), authority);
    }

    private Path transferToWrappedPath(Path path, TransferedPath newPath) {
        String currentPath = path.toUri().getPath();
        String fromPrefix = newPath.getMountInfo().getToPath(); // reverse path convert
        String targetPrefix = newPath.isReservedPath ? "/" : newPath.getMountInfo().getFromPath();

        String remainPart;
        if (currentPath.startsWith(fromPrefix)) {
            remainPart = currentPath.substring(fromPrefix.length());
            if (remainPart.startsWith(Path.SEPARATOR)) {
                remainPart = remainPart.substring(1);
            }
        } else {
            // in case the path is not in mount, transfer to wrapped path
            try {
                remainPart = toWrappedPath(new Path(currentPath), newPath.getFS()).toString();
            } catch (IOException e) {
                LOG.warn("failed in toWrappedPath", e);
                remainPart = currentPath;
            }
        }

        Path targetPrefixPath = new Path(getUri().getScheme(), getUri().getAuthority(), targetPrefix);
        return new Path(targetPrefixPath, remainPart);
    }

    public void initialize(URI theUri, Configuration conf) throws IOException {
        this.wrapperConf = new Configuration(conf);
        wrapperConf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        wrapperConf.setBoolean("fs.hdfs.impl.disable.cache", true);

        super.initialize(theUri, conf);
        underHDFS = (DistributedFileSystem) FileSystem.get(theUri, wrapperConf);

        final String authority = theUri.getAuthority();
        mountMap = new HashMap<>();
        for (Pair<String, String> p : initMountList(getMountList(conf, authority))) {
            String fromPath = new Path(p.getKey()).toString();
            Path toRawPath = new Path(p.getValue());
            String toPath = toRawPath.toUri().getPath();
            LOG.info("Initialize mount fs from " + fromPath + " to " + toRawPath);
            mountMap.put(p.getKey(), new MountInfo(fromPath, toPath, () -> {
                try {
                    return toRawPath.getFileSystem(wrapperConf);
                } catch (IOException e) {
                    throw new UncheckException(e);
                }
            }));
        }
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
        TransferedPath newPath = transferToNewPath(file.getPath());
        return newPath.getFS().getFileBlockLocations(newPath.toPath(), start, len);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path p, final long start, final long len) throws IOException {
        TransferedPath newPath = transferToNewPath(p);
        return newPath.getFS().getFileBlockLocations(newPath.toPath(), start, len);
    }

    @Override
    public boolean recoverLease(final Path path) throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        if (newPath.getFS() == underHDFS) {
            return underHDFS.recoverLease(path);
        }
        return true;
    }

    @Override
    public FSDataInputStream open(Path path, final int i) throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        return newPath.getFS().open(newPath.toPath(), i);
    }

    @Override
    public FSDataOutputStream append(Path f, final EnumSet<CreateFlag> flag, final int bufferSize,
        final Progressable progress) throws UnsupportedOperationException, IOException {
        TransferedPath newPath = transferToNewPath(f);
        return newPath.getFS().append(newPath.toPath(), bufferSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite, int bufferSize,
        short replication, long blockSize, Progressable progressable) throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        return newPath.getFS()
            .create(newPath.toPath(), fsPermission, overwrite, bufferSize, replication, blockSize, progressable);
    }

    @Override
    public HdfsDataOutputStream create(final Path f, final FsPermission permission, final boolean overwrite,
        final int bufferSize, final short replication, final long blockSize, final Progressable progress,
        final InetSocketAddress[] favoredNodes) throws UnsupportedOperationException, IOException {
        TransferedPath newPath = transferToNewPath(f);
        if (newPath.getFS() == underHDFS) {
            return underHDFS.create(newPath.toPath(), permission, overwrite, bufferSize, replication, blockSize,
                progress, favoredNodes);
        }
        throw new UnsupportedOperationException(
            "Not implemented create(final Path f, final FsPermission permission, final boolean overwrite, final int bufferSize, final short replication, final long blockSize, final Progressable progress, final InetSocketAddress[] favoredNodes)!");
    }

    @Override
    public FSDataOutputStream create(Path f, final FsPermission permission, final EnumSet<CreateFlag> cflags,
        final int bufferSize, final short replication, final long blockSize, final Progressable progress,
        final Options.ChecksumOpt checksumOpt) throws UnsupportedOperationException, IOException {
        TransferedPath newPath = transferToNewPath(f);
        return newPath.getFS()
            .create(newPath.toPath(), permission, cflags, bufferSize, replication, blockSize, progress, checksumOpt);
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path path, final FsPermission permission,
        final EnumSet<CreateFlag> flag, final int bufferSize, final short replication, final long blockSize,
        final Progressable progress) throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        return newPath.getFS()
            .createNonRecursive(newPath.toPath(), permission, flag, bufferSize, replication, blockSize, progress);
    }

    @Override
    public boolean setReplication(Path src, final short replication) throws IOException {
        TransferedPath newPath = transferToNewPath(src);
        return newPath.getFS().setReplication(newPath.toPath(), replication);
    }

    @Override
    public void setStoragePolicy(Path src, final String policyName) throws UnsupportedOperationException, IOException {
        TransferedPath newPath = transferToNewPath(src);
        if (newPath.getFS() == underHDFS) {
            underHDFS.setStoragePolicy(newPath.toPath(), policyName);
        } else {
            throw new UnsupportedOperationException(
                "Not implemented setStoragePolicy(Path src, final String policyName)!");
        }
    }

    @Override
    public void unsetStoragePolicy(final Path src) throws UnsupportedOperationException, IOException {
        TransferedPath newPath = transferToNewPath(src);
        if (newPath.getFS() == underHDFS) {
            underHDFS.unsetStoragePolicy(newPath.toPath());
        } else {
            throw new UnsupportedOperationException("Not implemented unsetStoragePolicy");
        }
    }

    @Override
    public BlockStoragePolicySpi getStoragePolicy(Path path) throws UnsupportedOperationException, IOException {
        TransferedPath newPath = transferToNewPath(path);
        if (newPath.getFS() == underHDFS) {
            return underHDFS.getStoragePolicy(newPath.toPath());
        }
        throw new UnsupportedOperationException("Not implemented getStoragePolicy(Path path)!");
    }

    @Override
    public void concat(Path trg, Path[] psrcs) throws IOException {
        TransferedPath newPath = transferToNewPath(trg);
        List<TransferedPath> newpsrcs = Arrays.stream(psrcs).map(this::transferToNewPath).collect(Collectors.toList());
        String targetAuthority = newPath.getFS().getUri().getAuthority();
        String targetSchema = newPath.getFS().getUri().getScheme();

        for (TransferedPath newpsrc : newpsrcs) {
            if (!newpsrc.getFS().getUri().getScheme().equals(targetSchema) || !newpsrc.getFS()
                .getUri()
                .getAuthority()
                .equals(targetAuthority)) {
                throw new UnsupportedOperationException(
                    "can not concat files across the filesystem, target filesystem: " + newPath.getFS().getUri()
                        + ", source: " + newpsrc.getFS().getUri());
            }
        }

        Path[] concatPaths = newpsrcs.stream().map(TransferedPath::toPath).toArray(Path[]::new);
        newPath.getFS().concat(newPath.toPath(), concatPaths);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        TransferedPath newPath = transferToNewPath(src);
        TransferedPath newPathDest = transferToNewPath(dst);

        // Objects.equals will handle the NULL authority case
        if (!Objects.equals(newPath.getFS().getUri().getAuthority(), newPathDest.getFS().getUri().getAuthority())) {
            throw new IOException(new UnsupportedOperationException(
                "can not support rename across filesystem. srcfs: " + newPath.getFS().getUri() + ", dsffs: "
                    + newPathDest.getFS().getUri()));
        }

        return newPath.getFS().rename(newPath.toPath(), newPathDest.toPath());
    }

    @Override
    public void rename(Path src, Path dst, final Options.Rename... options)
        throws UnsupportedOperationException, IOException {
        TransferedPath newPath = transferToNewPath(src);
        TransferedPath newPathDest = transferToNewPath(dst);

        if (!Objects.equals(newPath.getFS().getUri().getAuthority(), newPathDest.getFS().getUri().getAuthority())) {
            throw new IOException(new UnsupportedOperationException("can not support rename across filesystem"));
        }

        if (newPath.toPath().toString().equals(newPathDest.toPath().toString())) {
            return;
        }

        try {
            Class<?> classType = newPath.getFS().getClass();
            Method method = classType.getDeclaredMethod("rename", Path.class, Path.class, Options.Rename[].class);
            method.setAccessible(true);
            method.invoke(newPath.getFS(), new Object[] {newPath.toPath(), newPathDest.toPath(), options});
            return;
        } catch (NoSuchMethodException e) {
            // ignore, use rename function, go to below code
            LOG.warn("ignore, use rename function, go to below code", e);
        } catch (IllegalAccessException | InvocationTargetException e) {
            LOG.warn("use reflection rename failed, ignore this and use FileSystem.rename method.", e);
        }

        if (options.length > 0 && options[0] == Options.Rename.OVERWRITE) {
            newPath.getFS().delete(newPathDest.toPath(), false);
            newPath.getFS().rename(newPath.toPath(), newPathDest.toPath());
        } else {
            newPath.getFS().rename(newPath.toPath(), newPathDest.toPath());
        }
    }

    @Override
    public boolean truncate(Path f, final long newLength) throws IOException {
        TransferedPath newPath = transferToNewPath(f);
        return newPath.getFS().truncate(newPath.toPath(), newLength);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        return newPath.getFS().delete(newPath.toPath(), recursive);
    }

    @Override
    public ContentSummary getContentSummary(Path f) throws IOException {
        TransferedPath newPath = transferToNewPath(f);
        return newPath.getFS().getContentSummary(newPath.toPath());
    }

    @Override
    public QuotaUsage getQuotaUsage(Path f) throws UnsupportedOperationException, IOException {
        TransferedPath newPath = transferToNewPath(f);
        return newPath.getFS().getQuotaUsage(newPath.toPath());

    }

    @Override
    public void setQuota(Path src, final long namespaceQuota, final long storagespaceQuota)
        throws UnsupportedOperationException, IOException {
        TransferedPath newPath = transferToNewPath(src);
        if (newPath.getFS() == underHDFS) {
            underHDFS.setQuota(newPath.toPath(), namespaceQuota, storagespaceQuota);
        } else {
            throw new UnsupportedOperationException(
                "Not implemented setQuota(Path src, final long namespaceQuota, final long storagespaceQuota)!");
        }
    }

    @Override
    public void setQuotaByStorageType(Path src, final StorageType type, final long quota)
        throws UnsupportedOperationException, IOException {
        TransferedPath newPath = transferToNewPath(src);
        if (newPath.getFS() == underHDFS) {
            underHDFS.setQuotaByStorageType(newPath.toPath(), type, quota);
        } else {
            throw new UnsupportedOperationException(
                "Not implemented setQuotaByStorageType(Path src, final StorageType type, final long quota)!");
        }
    }

    @Override
    public FileStatus[] listStatus(Path p) throws IOException {
        TransferedPath newPath = transferToNewPath(p);
        if (newPath.getFS() == underHDFS) {
            return underHDFS.listStatus(newPath.toPath());
        }
        FileStatus[] fileStatuses = newPath.getFS().listStatus(newPath.toPath());
        for (int i = 0; i < fileStatuses.length; i++) {

            fileStatuses[i].setPath(transferToWrappedPath(fileStatuses[i].getPath(), newPath));
        }
        return fileStatuses;
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive)
        throws IOException {
        TransferedPath newPath = transferToNewPath(f);
        return new WrappedRemoteIterator<>(newPath.getFS().listFiles(newPath.toPath(), recursive),
            fileStatus -> {
                Path originPath = transferToWrappedPath(fileStatus.getPath(), newPath);
                fileStatus.setPath(originPath);
                return fileStatus;
            });
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path p, final PathFilter filter) throws IOException {
        TransferedPath newPath = transferToNewPath(p);
        return new WrappedRemoteIterator<>(newPath.getFS().listLocatedStatus(newPath.toPath()),
            fileStatus -> {
                Path originPath = transferToWrappedPath(fileStatus.getPath(), newPath);
                fileStatus.setPath(originPath);
                return fileStatus;
            });
    }

    @Override
    public RemoteIterator<FileStatus> listStatusIterator(Path p) throws IOException {
        TransferedPath newPath = transferToNewPath(p);
        return new WrappedRemoteIterator<>(newPath.getFS().listStatusIterator(newPath.toPath()),
            fileStatus -> {
                Path originPath = transferToWrappedPath(fileStatus.getPath(), newPath);
                fileStatus.setPath(originPath);
                return fileStatus;
            });
    }

    @Override
    public boolean mkdir(Path f, FsPermission permission) throws IOException {
        TransferedPath newPath = transferToNewPath(f);
        if (newPath.getFS() == underHDFS) {
            return underHDFS.mkdir(newPath.toPath(), permission);
        } else {
            return newPath.getFS().mkdirs(newPath.toPath(), permission);
        }
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        return newPath.getFS().mkdirs(newPath.toPath(), fsPermission);
    }

    @Override
    public void close() throws IOException {
        IOException ex = null;
        try {
            super.close();
        } catch (IOException e) {
            ex = e;
            LOG.error("failed to close", e);
        }
        for (MountInfo value : mountMap.values()) {
            try {
                if (value.toFileSystem != null) {
                    value.toFileSystem.close();
                }
            } catch (IOException e) {
                ex = e;
                LOG.error("failed to close " + value, e);
            }
        }

        if (underHDFS != null) {
            underHDFS.close();
        }

        if (ex != null) {
            throw ex;
        }
    }

    @Override
    public FsStatus getStatus(Path p) throws IOException {
        TransferedPath newPath = transferToNewPath(p);
        return newPath.getFS().getStatus(newPath.toPath());
    }

    @Override
    public RemoteIterator<Path> listCorruptFileBlocks(Path path) throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        return new WrappedRemoteIterator<>(newPath.getFS().listCorruptFileBlocks(newPath.toPath()),
            p -> transferToWrappedPath(p, newPath));
    }

    //此函数是为了判断文件或者文件夹的状态，并不是进行真实的操作
    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        TransferedPath newPath = transferToNewPath(path);

        FileStatus fileStatus = newPath.getFS().getFileStatus(newPath.toPath());
        //因为这一步走了好多坑，一定要把path再转化成原来的模样
        Optional.ofNullable(fileStatus).ifPresent(f -> f.setPath(path));
        return fileStatus;
    }

    @Override
    public void createSymlink(final Path target, Path link, final boolean createParent) throws IOException {
        TransferedPath newTarget = transferToNewPath(target);
        TransferedPath newLink = transferToNewPath(link);

        if (!newTarget.getFS().getUri().getAuthority().equals(newLink.getFS().getUri().getAuthority())) {
            throw new UnsupportedOperationException("can not support createSymlink across filesystem");
        }

        newLink.getFS().createSymlink(newTarget.toPath(), newLink.toPath(), createParent);
    }

    @Override
    public FileStatus getFileLinkStatus(Path f) throws IOException {
        TransferedPath newPath = transferToNewPath(f);
        if (newPath.getFS() == underHDFS) {
            return underHDFS.getFileLinkStatus(newPath.toPath());
        }
        FileStatus fileStatus = newPath.getFS().getFileLinkStatus(newPath.toPath());
        //因为这一步走了好多坑，一定要把path再转化成原来的模样
        Optional.ofNullable(fileStatus).ifPresent(fileStatus1 -> fileStatus1.setPath(f));
        return fileStatus;
    }

    @Override
    public Path getLinkTarget(Path f) throws IOException {
        TransferedPath newPath = transferToNewPath(f);
        if (newPath.getFS() == underHDFS) {
            return underHDFS.getLinkTarget(newPath.toPath());
        }
        return newPath.getFS().getLinkTarget(newPath.toPath());
    }

    @Override
    public FileChecksum getFileChecksum(Path f) throws IOException {
        TransferedPath newPath = transferToNewPath(f);
        return newPath.getFS().getFileChecksum(newPath.toPath());
    }

    @Override
    public FileChecksum getFileChecksum(Path f, final long length) throws IOException {
        TransferedPath newPath = transferToNewPath(f);
        return newPath.getFS().getFileChecksum(newPath.toPath(), length);
    }

    @Override
    public void setPermission(Path p, final FsPermission permission) throws IOException {
        TransferedPath newPath = transferToNewPath(p);
        newPath.getFS().setPermission(newPath.toPath(), permission);
    }

    public void setOwner(Path p, final String username, final String groupname)
        throws UnsupportedOperationException, IOException {
        TransferedPath newPath = transferToNewPath(p);
        newPath.getFS().setOwner(newPath.toPath(), username, groupname);
    }

    @Override
    public void setTimes(Path p, final long mtime, final long atime) throws UnsupportedOperationException, IOException {
        TransferedPath newPath = transferToNewPath(p);
        newPath.getFS().setTimes(newPath.toPath(), mtime, atime);
    }

    @Override
    public Path createSnapshot(final Path path, final String snapshotName) throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        return newPath.getFS().createSnapshot(newPath.toPath(), snapshotName);
    }

    @Override
    public void renameSnapshot(final Path path, final String snapshotOldName, final String snapshotNewName)
        throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        newPath.getFS().renameSnapshot(newPath.toPath(), snapshotOldName, snapshotNewName);
    }

    @Override
    public void deleteSnapshot(final Path snapshotDir, final String snapshotName) throws IOException {
        TransferedPath newPath = transferToNewPath(snapshotDir);
        newPath.getFS().deleteSnapshot(newPath.toPath(), snapshotName);
    }

    @Override
    public void modifyAclEntries(Path path, final List<AclEntry> aclSpec) throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        newPath.getFS().modifyAclEntries(newPath.toPath(), aclSpec);
    }

    @Override
    public void removeAclEntries(Path path, final List<AclEntry> aclSpec) throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        newPath.getFS().removeAclEntries(newPath.toPath(), aclSpec);
    }

    @Override
    public void removeDefaultAcl(Path path) throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        newPath.getFS().removeDefaultAcl(newPath.toPath());
    }

    @Override
    public void removeAcl(Path path) throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        newPath.getFS().removeAcl(newPath.toPath());
    }

    @Override
    public void setAcl(Path path, final List<AclEntry> aclSpec) throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        newPath.getFS().setAcl(newPath.toPath(), aclSpec);
    }

    @Override
    public AclStatus getAclStatus(Path path) throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        return newPath.getFS().getAclStatus(newPath.toPath());
    }

    @Override
    public void setXAttr(Path path, final String name, final byte[] value, final EnumSet<XAttrSetFlag> flag)
        throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        newPath.getFS().setXAttr(newPath.toPath(), name, value, flag);
    }

    @Override
    public byte[] getXAttr(Path path, final String name) throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        return newPath.getFS().getXAttr(newPath.toPath(), name);
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path) throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        return newPath.getFS().getXAttrs(newPath.toPath());
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path, final List<String> names) throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        return newPath.getFS().getXAttrs(newPath.toPath(), names);
    }

    public List<String> listXAttrs(Path path) throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        return newPath.getFS().listXAttrs(newPath.toPath());
    }

    @Override
    public void removeXAttr(Path path, final String name) throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        newPath.getFS().removeXAttr(newPath.toPath(), name);
    }

    @Override
    public void access(Path path, final FsAction mode) throws IOException {
        TransferedPath newPath = transferToNewPath(path);
        newPath.getFS().access(newPath.toPath(), mode);
    }

    @Override
    public Path getTrashRoot(Path path) {
        TransferedPath newPath = transferToNewPath(path);
        try {
            return toWrappedPath(newPath.getFS().getTrashRoot(newPath.toPath()), newPath.getFS());
        } catch (IOException e) {
            throw new UncheckException(e);
        }
    }

    private static class WrappedRemoteIterator<T> implements RemoteIterator<T> {

        private final RemoteIterator<T> origin;

        private final Function<T, T> convertFunc;

        WrappedRemoteIterator(RemoteIterator<T> origin, Function<T, T> convertFunc) {
            this.origin = origin;
            this.convertFunc = convertFunc;
        }

        @Override
        public boolean hasNext() throws IOException {
            return origin.hasNext();
        }

        @Override
        public T next() throws IOException {
            return convertFunc.apply(origin.next());
        }
    }

    static class MountInfo {
        String fromPath;

        String toPath;

        Supplier<FileSystem> toFileSystemSupplier;

        FileSystem toFileSystem = null;

        public MountInfo(String from, String to, Supplier<FileSystem> toFileSystem) {
            this.fromPath = from;
            this.toPath = to;
            this.toFileSystemSupplier = toFileSystem;
        }

        public String getFromPath() {
            return fromPath;
        }

        public String getToPath() {
            return toPath;
        }

        public FileSystem getToFileSystem() throws IOException {
            if (toFileSystem != null) {
                return toFileSystem;
            }
            if (toFileSystemSupplier != null) {
                initToFileSystem();
            }
            return toFileSystem;
        }

        private synchronized void initToFileSystem() throws IOException {
            if (toFileSystem == null) {
                try {
                    toFileSystem = toFileSystemSupplier.get();
                } catch (UncheckException e) {
                    throw e.getException();
                }
            }
        }
    }

    static class TransferedPath {
        private final boolean isReservedPath;

        MountInfo mountInfo;

        String remainPath;

        public TransferedPath(MountInfo mountInfo, String remainPath, boolean isReservedPath) {
            this.mountInfo = mountInfo;
            this.remainPath = remainPath;
            this.isReservedPath = isReservedPath;
        }

        public TransferedPath(MountInfo mountInfo, String remainPath) {
            this(mountInfo, remainPath, false);
        }

        public MountInfo getMountInfo() {
            return mountInfo;
        }

        public String getRemainPath() {
            return remainPath;
        }

        /**
         * get the mounted filesystem
         * if the mounted filesystem is current MRSHDFSWrapperFileSystem instance,
         * use MRSHDFSWrapperFileSystem.super implementation
         *
         * @return filesystem in mount
         * @throws IOException
         */
        public FileSystem getFS() throws IOException {
            return mountInfo.getToFileSystem();
        }

        public Path toPath() {
            if (remainPath == null || remainPath.trim().length() == 0) {
                return new Path(mountInfo.toPath);
            } else if (isReservedPath) {
                return new Path(remainPath);
            } else {
                return remainPath.length() > 1
                    ? new Path(mountInfo.toPath, remainPath.substring(1))
                    : new Path(mountInfo.toPath);
            }
        }
    }

    static class UncheckException extends RuntimeException {
        static final long serialVersionUID = 5746198432791324945L;

        public UncheckException(IOException origin) {
            super(origin);
        }

        public IOException getException() {
            return (IOException) getCause();
        }
    }

}
