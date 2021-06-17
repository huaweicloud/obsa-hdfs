/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.obs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * this class copy from hadoop-common module.
 * in flink or other long run application,OBSAllocatorPerContext#createTmpFileForWrite method invoke
 * File.deleteOnExit,and will cause JVM OOM.
 * <p>
 * JDK-4872014:The File.deleteOnExit API is flawed and causes JVM crashes
 * on long running servers (or even short running if the API is used enough). See bug 4513817.
 */

public class OBSLocalDirAllocator {

    //A Map from the config item names like "mapred.local.dir"
    //to the instance of the AllocatorPerContext. This
    //is a static object to make sure there exists exactly one instance per JVM
    private static Map<String, OBSAllocatorPerContext> contexts =
        new TreeMap<String, OBSAllocatorPerContext>();

    private String contextCfgItemName;

    /**
     * Used when size of file to be allocated is unknown.
     */
    public static final int SIZE_UNKNOWN = -1;

    /**
     * Create an allocator object
     *
     * @param contextCfgItemName
     */
    public OBSLocalDirAllocator(String contextCfgItemName) {
        this.contextCfgItemName = contextCfgItemName;
    }

    /**
     * This method must be used to obtain the dir allocation context for a
     * particular value of the context name. The context name must be an item
     * defined in the Configuration object for which we want to control the
     * dir allocations (e.g., <code>mapred.local.dir</code>). The method will
     * create a context for that name if it doesn't already exist.
     */
    private OBSAllocatorPerContext obtainContext(String contextCfgItemName) {
        synchronized (contexts) {
            OBSAllocatorPerContext l = contexts.get(contextCfgItemName);
            if (l == null) {
                contexts.put(contextCfgItemName, l = new OBSAllocatorPerContext(contextCfgItemName));
            }
            return l;
        }
    }

    /**
     * Get a path from the local FS. This method should be used if the size of
     * the file is not known apriori. We go round-robin over the set of disks
     * (via the configured dirs) and return the first complete path where
     * we could create the parent directory of the passed path.
     *
     * @param pathStr the requested path (this will be created on the first
     *                available disk)
     * @param conf    the Configuration object
     * @return the complete path to the file on a local disk
     * @throws IOException
     */
    public Path getLocalPathForWrite(String pathStr,
        Configuration conf) throws IOException {
        return getLocalPathForWrite(pathStr, SIZE_UNKNOWN, conf);
    }

    /**
     * Get a path from the local FS. Pass size as
     * SIZE_UNKNOWN if not known apriori. We
     * round-robin over the set of disks (via the configured dirs) and return
     * the first complete path which has enough space
     *
     * @param pathStr the requested path (this will be created on the first
     *                available disk)
     * @param size    the size of the file that is going to be written
     * @param conf    the Configuration object
     * @return the complete path to the file on a local disk
     * @throws IOException
     */
    public Path getLocalPathForWrite(String pathStr, long size,
        Configuration conf) throws IOException {
        return getLocalPathForWrite(pathStr, size, conf, true);
    }

    /**
     * Get a path from the local FS. Pass size as
     * SIZE_UNKNOWN if not known apriori. We
     * round-robin over the set of disks (via the configured dirs) and return
     * the first complete path which has enough space
     *
     * @param pathStr    the requested path (this will be created on the first
     *                   available disk)
     * @param size       the size of the file that is going to be written
     * @param conf       the Configuration object
     * @param checkWrite ensure that the path is writable
     * @return the complete path to the file on a local disk
     * @throws IOException
     */
    public Path getLocalPathForWrite(String pathStr, long size,
        Configuration conf,
        boolean checkWrite) throws IOException {
        OBSAllocatorPerContext context = obtainContext(contextCfgItemName);
        return context.getLocalPathForWrite(pathStr, size, conf, checkWrite);
    }

    /**
     * Get a path from the local FS for reading. We search through all the
     * configured dirs for the file's existence and return the complete
     * path to the file when we find one
     *
     * @param pathStr the requested file (this will be searched)
     * @param conf    the Configuration object
     * @return the complete path to the file on a local disk
     * @throws IOException
     */
    public Path getLocalPathToRead(String pathStr,
        Configuration conf) throws IOException {
        OBSAllocatorPerContext context = obtainContext(contextCfgItemName);
        return context.getLocalPathToRead(pathStr, conf);
    }

    /**
     * Get all of the paths that currently exist in the working directories.
     *
     * @param pathStr the path underneath the roots
     * @param conf    the configuration to look up the roots in
     * @return all of the paths that exist under any of the roots
     * @throws IOException
     */
    public Iterable<Path> getAllLocalPathsToRead(String pathStr,
        Configuration conf
    ) throws IOException {
        OBSAllocatorPerContext context;
        synchronized (this) {
            context = obtainContext(contextCfgItemName);
        }
        return context.getAllLocalPathsToRead(pathStr, conf);
    }

    /**
     * Creates a temporary file in the local FS. Pass size as -1 if not known
     * apriori. We round-robin over the set of disks (via the configured dirs)
     * and select the first complete path which has enough space. A file is
     * created on this directory. The file is guaranteed to go away when the
     * JVM exits.
     *
     * @param pathStr prefix for the temporary file
     * @param size    the size of the file that is going to be written
     * @param conf    the Configuration object
     * @return a unique temporary file
     * @throws IOException
     */
    public File createTmpFileForWrite(String pathStr, long size,
        Configuration conf) throws IOException {
        OBSAllocatorPerContext context = obtainContext(contextCfgItemName);
        return context.createTmpFileForWrite(pathStr, size, conf);
    }

    /**
     * Method to check whether a context is valid
     *
     * @param contextCfgItemName
     * @return true/false
     */
    public static boolean isContextValid(String contextCfgItemName) {
        synchronized (contexts) {
            return contexts.containsKey(contextCfgItemName);
        }
    }

    /**
     * Removes the context from the context config items
     *
     * @param contextCfgItemName
     */
    @Deprecated
    @InterfaceAudience.LimitedPrivate({"MapReduce"})
    public static void removeContext(String contextCfgItemName) {
        synchronized (contexts) {
            contexts.remove(contextCfgItemName);
        }
    }

    /**
     * We search through all the configured dirs for the file's existence
     * and return true when we find
     *
     * @param pathStr the requested file (this will be searched)
     * @param conf    the Configuration object
     * @return true if files exist. false otherwise
     * @throws IOException
     */
    public boolean ifExists(String pathStr, Configuration conf) {
        OBSAllocatorPerContext context = obtainContext(contextCfgItemName);
        return context.ifExists(pathStr, conf);
    }

    /**
     * Get the current directory index for the given configuration item.
     *
     * @return the current directory index for the given configuration item.
     */
    int getCurrentDirectoryIndex() {
        OBSAllocatorPerContext context = obtainContext(contextCfgItemName);
        return context.getCurrentDirectoryIndex();
    }

    private static class OBSAllocatorPerContext {

        private final Log log = LogFactory.getLog(OBSAllocatorPerContext.class);

        private Random dirIndexRandomizer = new Random();

        private String contextCfgItemName;

        // NOTE: the context must be accessed via a local reference as it
        //       may be updated at any time to reference a different context
        private AtomicReference<Context> currentContext;

        private static class Context {
            private AtomicInteger dirNumLastAccessed = new AtomicInteger(0);

            private FileSystem localFS;

            private DF[] dirDF;

            private Path[] localDirs;

            private String savedLocalDirs;

            public int getAndIncrDirNumLastAccessed() {
                return getAndIncrDirNumLastAccessed(1);
            }

            public int getAndIncrDirNumLastAccessed(int delta) {
                if (localDirs.length < 2 || delta == 0) {
                    return dirNumLastAccessed.get();
                }
                int oldval;
                int newval;
                do {
                    oldval = dirNumLastAccessed.get();
                    newval = (oldval + delta) % localDirs.length;
                } while (!dirNumLastAccessed.compareAndSet(oldval, newval));
                return oldval;
            }
        }

        public OBSAllocatorPerContext(String contextCfgItemName) {
            this.contextCfgItemName = contextCfgItemName;
            this.currentContext = new AtomicReference<Context>(new Context());
        }

        /**
         * This method gets called everytime before any read/write to make sure
         * that any change to localDirs is reflected immediately.
         */
        private Context confChanged(Configuration conf)
            throws IOException {
            Context ctx = currentContext.get();
            String newLocalDirs = conf.get(contextCfgItemName);
            if (null == newLocalDirs) {
                throw new IOException(contextCfgItemName + " not configured");
            }
            if (!newLocalDirs.equals(ctx.savedLocalDirs)) {
                ctx = confChanged2(conf, newLocalDirs);
            }

            return ctx;
        }

        private Context confChanged2(Configuration conf, String newLocalDirs) throws IOException {
            Context ctx = new Context();
            String[] dirStrings = StringUtils.getTrimmedStrings(newLocalDirs);
            ctx.localFS = FileSystem.getLocal(conf);
            int numDirs = dirStrings.length;
            ArrayList<Path> dirs = new ArrayList<Path>(numDirs);
            ArrayList<DF> dfList = new ArrayList<DF>(numDirs);
            for (int i = 0; i < numDirs; i++) {
                try {
                    // filter problematic directories
                    Path tmpDir = new Path(dirStrings[i]);
                    if (ctx.localFS.mkdirs(tmpDir) || ctx.localFS.exists(tmpDir)) {
                        try {
                            File tmpFile = tmpDir.isAbsolute()
                                ? new File(ctx.localFS.makeQualified(tmpDir).toUri())
                                : new File(dirStrings[i]);

                            DiskChecker.checkDir(tmpFile);
                            dirs.add(new Path(tmpFile.getPath()));
                            dfList.add(new DF(tmpFile, 30000));
                        } catch (DiskErrorException de) {
                            log.warn(dirStrings[i] + " is not writable\n", de);
                        }
                    } else {
                        log.warn("Failed to create " + dirStrings[i]);
                    }
                } catch (IOException ie) {
                    log.warn("Failed to create " + dirStrings[i] + ": "
                        + ie.getMessage() + "\n", ie);
                } //ignore
            }
            ctx.localDirs = dirs.toArray(new Path[dirs.size()]);
            ctx.dirDF = dfList.toArray(new DF[dirs.size()]);
            ctx.savedLocalDirs = newLocalDirs;

            if (dirs.size() > 0) {
                // randomize the first disk picked in the round-robin selection
                ctx.dirNumLastAccessed.set(dirIndexRandomizer.nextInt(dirs.size()));
            }

            currentContext.set(ctx);

            return ctx;
        }

        private Path createPath(Path dir, String path,
            boolean checkWrite) throws IOException {
            Path file = new Path(dir, path);
            if (checkWrite) {
                //check whether we are able to create a directory here. If the disk
                //happens to be RDONLY we will fail
                try {
                    DiskChecker.checkDir(new File(file.getParent().toUri().getPath()));
                    return file;
                } catch (DiskErrorException d) {
                    log.warn("Disk Error Exception: ", d);
                    return null;
                }
            }
            return file;
        }

        /**
         * Get the current directory index.
         *
         * @return the current directory index.
         */
        int getCurrentDirectoryIndex() {
            return currentContext.get().dirNumLastAccessed.get();
        }

        /**
         * Get a path from the local FS. If size is known, we go
         * round-robin over the set of disks (via the configured dirs) and return
         * the first complete path which has enough space.
         * <p>
         * If size is not known, use roulette selection -- pick directories
         * with probability proportional to their available space.
         */
        public Path getLocalPathForWrite(String pathStr, long size,
            Configuration conf, boolean checkWrite) throws IOException {
            Context ctx = confChanged(conf);
            int numDirs = ctx.localDirs.length;
            int numDirsSearched = 0;
            //remove the leading slash from the path (to make sure that the uri
            //resolution results in a valid path on the dir being checked)
            if (pathStr.startsWith("/")) {
                pathStr = pathStr.substring(1);
            }
            Path returnPath = null;

            if (size == SIZE_UNKNOWN) {  //do roulette selection: pick dir with probability
                //proportional to available size
                long[] availableOnDisk = new long[ctx.dirDF.length];
                long totalAvailable = 0;

                //build the "roulette wheel"
                for (int i = 0; i < ctx.dirDF.length; ++i) {
                    availableOnDisk[i] = ctx.dirDF[i].getAvailable();
                    totalAvailable += availableOnDisk[i];
                }

                if (totalAvailable == 0) {
                    throw new DiskErrorException("No space available in any of the local directories.");
                }

                // Keep rolling the wheel till we get a valid path
                SecureRandom r = new SecureRandom();
                // Random r = new Random();
                while (numDirsSearched < numDirs && returnPath == null) {
                    long randomPosition = (r.nextLong() >>> 1) % totalAvailable;
                    int dir = 0;
                    while (randomPosition > availableOnDisk[dir]) {
                        randomPosition -= availableOnDisk[dir];
                        dir++;
                    }
                    ctx.dirNumLastAccessed.set(dir);
                    returnPath = createPath(ctx.localDirs[dir], pathStr, checkWrite);
                    if (returnPath == null) {
                        totalAvailable -= availableOnDisk[dir];
                        availableOnDisk[dir] = 0; // skip this disk
                        numDirsSearched++;
                    }
                }
            } else {
                int dirNum = ctx.getAndIncrDirNumLastAccessed();
                while (numDirsSearched < numDirs) {
                    long capacity = ctx.dirDF[dirNum].getAvailable();
                    if (capacity > size) {
                        returnPath =
                            createPath(ctx.localDirs[dirNum], pathStr, checkWrite);
                        if (returnPath != null) {
                            ctx.getAndIncrDirNumLastAccessed(numDirsSearched);
                            break;
                        }
                    }
                    dirNum++;
                    dirNum = dirNum % numDirs;
                    numDirsSearched++;
                }
            }
            if (returnPath != null) {
                return returnPath;
            }

            //no path found
            throw new DiskErrorException("Could not find any valid local "
                + "directory for " + pathStr);
        }

        /**
         * Creates a file on the local FS. Pass size as
         * {@link OBSLocalDirAllocator#SIZE_UNKNOWN} if not known apriori. We
         * round-robin over the set of disks (via the configured dirs) and return
         * a file on the first path which has enough space. The file is guaranteed
         * to go away when the JVM exits.
         */
        public File createTmpFileForWrite(String pathStr, long size,
            Configuration conf) throws IOException {

            // find an appropriate directory
            Path path = getLocalPathForWrite(pathStr, size, conf, true);
            File dir = new File(path.getParent().toUri().getPath());
            String prefix = path.getName();

            // create a temp file on this directory
            File result = File.createTempFile(prefix, null, dir);

            return result;
        }

        /**
         * Get a path from the local FS for reading. We search through all the
         * configured dirs for the file's existence and return the complete
         * path to the file when we find one
         */
        public Path getLocalPathToRead(String pathStr,
            Configuration conf) throws IOException {
            Context ctx = confChanged(conf);
            int numDirs = ctx.localDirs.length;
            int numDirsSearched = 0;
            //remove the leading slash from the path (to make sure that the uri
            //resolution results in a valid path on the dir being checked)
            if (pathStr.startsWith("/")) {
                pathStr = pathStr.substring(1);
            }
            while (numDirsSearched < numDirs) {
                Path file = new Path(ctx.localDirs[numDirsSearched], pathStr);
                if (ctx.localFS.exists(file)) {
                    return file;
                }
                numDirsSearched++;
            }

            //no path found
            throw new DiskErrorException("Could not find " + pathStr + " in any of"
                + " the configured local directories");
        }

        private static class PathIterator implements Iterator<Path>, Iterable<Path> {
            private final FileSystem fs;

            private final String pathStr;

            private int i = 0;

            private final Path[] rootDirs;

            private Path next = null;

            private PathIterator(FileSystem fs, String pathStr, Path[] rootDirs)
                throws IOException {
                this.fs = fs;
                this.pathStr = pathStr;
                this.rootDirs = rootDirs;
                advance();
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            private void advance() throws IOException {
                while (i < rootDirs.length) {
                    next = new Path(rootDirs[i++], pathStr);
                    if (fs.exists(next)) {
                        return;
                    }
                }
                next = null;
            }

            @Override
            public Path next() {
                final Path result = next;
                try {
                    advance();
                } catch (IOException ie) {
                    throw new RuntimeException("Can't check existence of " + next, ie);
                }
                if (result == null) {
                    throw new NoSuchElementException();
                }
                return result;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("read only iterator");
            }

            @Override
            public Iterator<Path> iterator() {
                return this;
            }
        }

        /**
         * Get all of the paths that currently exist in the working directories.
         *
         * @param pathStr the path underneath the roots
         * @param conf    the configuration to look up the roots in
         * @return all of the paths that exist under any of the roots
         * @throws IOException
         */
        Iterable<Path> getAllLocalPathsToRead(String pathStr,
            Configuration conf) throws IOException {
            Context ctx = confChanged(conf);
            if (pathStr.startsWith("/")) {
                pathStr = pathStr.substring(1);
            }
            return new PathIterator(ctx.localFS, pathStr, ctx.localDirs);
        }

        /**
         * We search through all the configured dirs for the file's existence
         * and return true when we find one
         */
        public boolean ifExists(String pathStr, Configuration conf) {
            Context ctx = currentContext.get();
            try {
                int numDirs = ctx.localDirs.length;
                int numDirsSearched = 0;
                //remove the leading slash from the path (to make sure that the uri
                //resolution results in a valid path on the dir being checked)
                if (pathStr.startsWith("/")) {
                    pathStr = pathStr.substring(1);
                }
                while (numDirsSearched < numDirs) {
                    Path file = new Path(ctx.localDirs[numDirsSearched], pathStr);
                    if (ctx.localFS.exists(file)) {
                        return true;
                    }
                    numDirsSearched++;
                }
            } catch (IOException e) {
                log.error(e);
            }
            return false;
        }
    }
}
