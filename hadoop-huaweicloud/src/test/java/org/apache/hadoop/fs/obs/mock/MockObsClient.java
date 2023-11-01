package org.apache.hadoop.fs.obs.mock;

import com.obs.services.IObsCredentialsProvider;
import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.exception.ObsException;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;
import com.obs.services.model.fs.ContentSummaryFsRequest;
import com.obs.services.model.fs.ContentSummaryFsResult;
import com.obs.services.model.fs.DirContentSummary;
import com.obs.services.model.fs.DirSummary;
import com.obs.services.model.fs.ListContentSummaryFsRequest;
import com.obs.services.model.fs.ListContentSummaryFsResult;

import org.apache.hadoop.fs.obs.OBSCommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class MockObsClient extends ObsClient {

    private static final Logger LOG = LoggerFactory.getLogger(MockObsClient.class);

    private Function<ListContentSummaryFsResult, ListContentSummaryFsResult> listCSResultCallback;

    private boolean getCSUnsupported;

    private int getCSNum;

    private int responseCode;

    private String errorMsg;

    public MockObsClient(IObsCredentialsProvider credentialsProvider, ObsConfiguration conf) {
        super(credentialsProvider, conf);
    }

    public void setListCSResultCallback(Function<ListContentSummaryFsResult, ListContentSummaryFsResult> callback) {
        this.listCSResultCallback = callback;
    }

    /**
     * use list to simulate listContentSummaryFs.
     * ignore the file in dir.
     * for each dir:
     * use {fakeSubDirNumPerDir} to fill #dir, this may not equal to actual dir
     * use {fakeFileNumPerDir} to fill #file
     * use {fakeFileSize} to fill #filesize
     * @param request
     * @return
     * @throws ObsException
     */
    @Override
    public ListContentSummaryFsResult listContentSummaryFs(ListContentSummaryFsRequest request) throws ObsException {
        ListContentSummaryFsResult res = new ListContentSummaryFsResult();
        List<DirContentSummary> dirContentSummaryList = new ArrayList<>();
        for (ListContentSummaryFsRequest.DirLayer dir :request.getDirLayers()) {
            DirContentSummary dirContentSummary = listSingleContentSummaryFs(request.getBucketName(), request.getMaxKeys(), dir);
            dirContentSummaryList.add(dirContentSummary);
        }
        res.setDirContentSummaries(dirContentSummaryList);
        if (this.listCSResultCallback != null) {
            return this.listCSResultCallback.apply(res);
        }
        return res;
    }

    private DirContentSummary listSingleContentSummaryFs(String bucket, int maxKeys, ListContentSummaryFsRequest.DirLayer dir) {
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucket);
        listObjectsRequest.setPrefix(maybeAddTrailingSlash(dir.getKey()));
        listObjectsRequest.setMarker(dir.getMarker());
        listObjectsRequest.setMaxKeys(maxKeys);
        listObjectsRequest.setDelimiter("/");
        ObjectListing objectListing = listObjects(listObjectsRequest);
        DirContentSummary ret = new DirContentSummary();
        List<DirSummary> dirSummaries = new ArrayList<>();
        ret.setKey(dir.getKey());
        if (objectListing.isTruncated()) {
            ret.setTruncated(true);
            ret.setNextMarker(objectListing.getNextMarker());
        }
        for (String dirname :objectListing.getCommonPrefixes()) {
            ContentSummaryFsRequest req = new ContentSummaryFsRequest();
            req.setBucketName(bucket);
            req.setDirName(dirname);
            ContentSummaryFsResult dirResult = getContentSummaryFs(req);
            dirSummaries.add(dirResult.getContentSummary());
        }
        ret.setSubDir(dirSummaries);
        return ret;
    }

    public ContentSummaryFsResult getContentSummaryFsUnsuppored(ContentSummaryFsRequest request) throws ObsException {
        ObsException e = new ObsException("mock exception: unsupported");
        e.setResponseCode(responseCode);
        e.setErrorMessage(errorMsg);
        getCSNum++;
        LOG.warn("mock exception: {}", errorMsg);
        throw e;
    }

    /**
     * use getFileStatus to simulate getContentSummaryFs.
     * ignore the file in dir.
     * use {fakeSubDirNumPerDir} to fill #dir, this may not equal to actual dir
     * use {fakeFileNumPerDir} to fill #file
     * use {fakeFileSize} to fill #filesize
     * @param request
     * @return
     * @throws ObsException
     */
    @Override
    public ContentSummaryFsResult getContentSummaryFs(ContentSummaryFsRequest request) throws ObsException {
        if (isGetCSUnsupported()) {
            return getContentSummaryFsUnsuppored(request);
        }
        long dNum = 0;
        long fNum = 0;
        long fSize = 0;
        String marker = null;
        boolean truncated;
        do {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
            listObjectsRequest.setBucketName(request.getBucketName());
            listObjectsRequest.setPrefix(maybeAddTrailingSlash(request.getDirName()));
            listObjectsRequest.setMaxKeys(1000);
            listObjectsRequest.setMarker(marker);
            listObjectsRequest.setDelimiter("/");
            ObjectListing objectListing = listObjects(listObjectsRequest);
            truncated = objectListing.isTruncated();
            marker = objectListing.getNextMarker();
            dNum += objectListing.getCommonPrefixes().size();
            for (ObsObject obj : objectListing.getObjects()) {
                if (obj.getObjectKey().equals(maybeAddTrailingSlash(request.getDirName()))) {
                    continue;
                }
                fSize += obj.getMetadata().getContentLength();
                fNum ++;
            }
        } while(truncated);

        ContentSummaryFsResult res = new ContentSummaryFsResult();
        DirSummary dirSummary = new DirSummary();
        dirSummary.setDirCount(dNum);
        dirSummary.setFileCount(fNum);
        dirSummary.setFileSize(fSize);
        dirSummary.setName(request.getDirName());
        res.setContentSummary(dirSummary);
        return res;
    }

    private String maybeAddTrailingSlash(final String key) {
        if (OBSCommonUtils.isStringNotEmpty(key) && !key.endsWith("/")) {
            return key + '/';
        } else {
            return key;
        }
    }

    public int getGetCSNum() {
        return getCSNum;
    }

    public void setGetCSNum(int getCSNum) {
        this.getCSNum = getCSNum;
    }

    public boolean isGetCSUnsupported() {
        return getCSUnsupported;
    }

    public void setGetCSUnsupported(boolean getCSUnsupported) {
        this.getCSUnsupported = getCSUnsupported;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(int responseCode) {
        this.responseCode = responseCode;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

}
