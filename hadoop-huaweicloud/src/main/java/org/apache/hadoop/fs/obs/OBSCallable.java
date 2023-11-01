package org.apache.hadoop.fs.obs;

import java.io.IOException;

/**
 * description
 *
 * @since 2022-04-13
 */
@FunctionalInterface
public interface OBSCallable<T> {

    T call() throws IOException;
}
