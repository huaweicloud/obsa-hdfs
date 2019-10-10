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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.*;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;

/**
 * Use {@link Constants#FAST_UPLOAD_BUFFER_DISK} for buffering.
 */
public class ITestOBSReadaheadInputStream extends ITestOBSBlockOutputArray {

  private FileSystem fs;

  private static final Logger LOG =
          LoggerFactory.getLogger(ITestOBSReadaheadInputStream.class);

  private static String testRootPath;
  @Rule
  public Timeout testTimeout = new Timeout(30 * 60 * 1000);

  @Before
  public void setup() throws Exception {
    Configuration conf = new Configuration();
    fs = createTestFileSystem(conf);
    //testRootPath = generateUniqueTestPath(conf);
  }


  @After
  public void teardown() throws Exception {
    if (fs != null) {
      //fs.delete(new Path(testRootPath), true);
    }
  }





  public static OBSFileSystem createTestFileSystem(Configuration configuration) throws IOException{
    String name = configuration.getTrimmed(TEST_FS_OBS_NAME);
    boolean liveTest = StringUtils.isEmpty(name);
    URI testURI = null;


    testURI = URI.create(name);
    Assert.assertEquals(testURI.getScheme(), Constants.FS_OBS);

    OBSFileSystem fs = new OBSFileSystem();
    fs.initialize(testURI,configuration);

    return fs;
  }

  public static String generateUniqueTestPath(Configuration configuration){
    String testUniquePathId = configuration.getTrimmed("test.unique.path.id","123");
    return testUniquePathId == null ? "/test" : "/" + testUniquePathId + "/test";
  }


  public static long generateFile(FileSystem fs, Path path, final long size, final int bufferLenth, final int modules) throws IOException{
    final byte[] buffer = new byte[bufferLenth];
    genTestBuffer(buffer,bufferLenth);

    long written=0;
    try (OutputStream outputStream = fs.create(path,true)) {
      while (written < size){
        final long diff =size -written;
        if (diff < bufferLenth){
          outputStream.write(buffer,0,(int) diff);
          written+=diff;
        } else {
          outputStream.write(buffer);
          written+=buffer.length;
        }
      }
      outputStream.flush();
    }
    return written;
  }

  private static void genTestBuffer(byte[] buffer ,long lenth){
    for (int i = 0; i< lenth;i++){
      buffer[i] = (byte)( i % 255);
    }
  }


  private Path setPath(String path) {
    if (path.startsWith("/")) {
      return new Path(testRootPath + path);
    } else {
      return new Path(testRootPath + "/" + path);
    }
  }

/*
  @Test
  public void testSeekFile() throws Exception {
    LOG.warn("Seeking file");
    Path smallSeekFile = setPath("/test/smallSeekFile.txt");
    long size = 10 * 1024;

    generateFile(this.fs, smallSeekFile, size, 256, 255);
    LOG.info("5MB file created: smallSeekFile.txt");

    FSDataInputStream instream = this.fs.open(smallSeekFile);
    int seekTimes = 5;
    LOG.info("multiple fold position seeking test...:");
    for (int i = 0; i < seekTimes; i++) {
      long pos = size / (seekTimes - i) - 1;
      LOG.info("begin seeking for pos: " + pos);
      instream.seek(pos);
      assertTrue("expected position at:" + pos + ", but got:"
              + instream.getPos(), instream.getPos() == pos);
      LOG.info("completed seeking at pos: " + instream.getPos());
    }
    LOG.info("random position seeking test...:");
    Random rand = new Random();
    for (int i = 0; i < seekTimes; i++) {
      long pos = Math.abs(rand.nextLong()) % size;
      LOG.info("begin seeking for pos: " + pos);
      instream.seek(pos);
      assertTrue("expected position at:" + pos + ", but got:"
              + instream.getPos(), instream.getPos() == pos);
      LOG.info("completed seeking at pos: " + instream.getPos());
    }
    IOUtils.closeStream(instream);
  }
*/


    @Test
    public void testSequentialAndRandomRead() throws Exception {
        Path smallSeekFile = setPath("/test/smallSeekFile.txt");
        long size = 5 * 1024 * 1024;

        //generateFile(this.fs, smallSeekFile, size, 256, 255);
        LOG.info("5MB file created: smallSeekFile.txt");


        //System.exit(0);

        long now= System.currentTimeMillis();
        FSDataInputStream fsDataInputStream = this.fs.open(smallSeekFile);
        OBSReadaheadInputStream in =
                (OBSReadaheadInputStream)fsDataInputStream.getWrappedStream();
        assertTrue("expected position at:" + 0 + ", but got:"
                + fsDataInputStream.getPos(), fsDataInputStream.getPos() == 0);

        LOG.warn("read one byte");
        byte[] bytes=new byte[(int)size];


        in.read(bytes,0, (int) (size));
        System.out.println(DatatypeConverter.printHexBinary(bytes));


        LOG.warn("time consumed mills："+String.valueOf(System.currentTimeMillis()-now));
        byte[] testBuffer = new byte[256];
        genTestBuffer(testBuffer,256);


        byte[] equalSizeBuffer= new byte[(int) size];

        for (int i=0;i<equalSizeBuffer.length;i++){
          equalSizeBuffer[i] = testBuffer[(i) % 256];
        }
        assertTrue(Arrays.equals(bytes,equalSizeBuffer));


        in.close();
        //fsDataInputStream.seek(4 * 1024 * 1024);


    }


    private void assertBufferOrder(Deque<ReadBuffer> buffers){
        long lastEnd=-1;
        Iterator<ReadBuffer> iterator=buffers.iterator();
        if (iterator.hasNext()){
            ReadBuffer readBuffer=iterator.next();
            lastEnd=readBuffer.getEnd();
        }

        while (iterator.hasNext()){
            ReadBuffer readBuffer=iterator.next();
            assertTrue("expect buffer start > last end but start: "+readBuffer.getStart()+" end: "+lastEnd,readBuffer.getStart()>lastEnd);
            assertTrue("expect buffer start - last end ==1 but start: "+readBuffer.getStart()+" end: "+lastEnd,readBuffer.getStart()-lastEnd==1);
            lastEnd=readBuffer.getEnd();
        }
    }

    private void assertBufferGood(OBSReadaheadInputStream s3AInputStream){
        assertBufferOrder(s3AInputStream.getBuffers());
    }
    @Test
    public void testSeek() throws Exception {
        Path smallSeekFile = setPath("/test/smallSeekFile.txt");


        //long size = 5* 1024 * 1024;
        //generateFile(this.fs, smallSeekFile, size, 256, 255);
        LOG.info("5MB file created: smallSeekFile.txt");


        long now= System.currentTimeMillis();


        long bufsize = 333;
        FSDataInputStream fsDataInputStream1 = this.fs.open(smallSeekFile);
        OBSReadaheadInputStream in1 =
                (OBSReadaheadInputStream)fsDataInputStream1.getWrappedStream();



        FSDataInputStream fsDataInputStream = this.fs.open(smallSeekFile);
        byte[] outbytes=new byte[(int)bufsize];
        byte[] outbytes2=new byte[(int)bufsize];
        byte[] outbytes3=new byte[(int)bufsize];
        in1.seek(2*1024*1024 + 900);
        in1.read(outbytes,0,(int)(bufsize));
        assertBufferGood(in1);
        in1.seek(400);
        in1.read(outbytes2,0, (int) bufsize);
        assertBufferGood(in1);
        in1.seek(4*1024*1024 - 5000);
        in1.read(outbytes3,0, (int) bufsize);
        assertBufferGood(in1);
        in1.close();

        OBSReadaheadInputStream in =
                (OBSReadaheadInputStream)fsDataInputStream.getWrappedStream();
        assertTrue("expected position at:" + 0 + ", but got:"
                + fsDataInputStream.getPos(), fsDataInputStream.getPos() == 0);

        LOG.warn("read one byte");



        byte[] bytes=new byte[(int)bufsize];

        in.seek(400);
        in.read(bytes,0, (int) (bufsize));
        assertBufferGood(in);

        byte[] bytesrewind= new byte[(int)(bufsize)];
        in.seek(400);

        in.read(bytesrewind,0 , (int)(bufsize));
        assertBufferGood(in);

        assertTrue("expect in buf backward seek true",Arrays.equals(bytes,bytesrewind));
        assertTrue("expect in buf backward seek true",Arrays.equals(bytes,outbytes2));




        in.seek(2*1024*1024 + 900);
        in.read(bytes,0, (int) (bufsize));
        assertBufferGood(in);

        in.seek(2*1024*1024 + 900);
        assertBufferGood(in);

        in.read(bytesrewind,0 , (int)(bufsize));
        assertBufferGood(in);

        assertTrue("expect accross buf backward seek true",Arrays.equals(bytes,bytesrewind));
        assertTrue("expect accross buf backward seek true",Arrays.equals(bytes,outbytes));


        in.seek(2*1024*1024);
        assertBufferGood(in);
        in.read(bytesrewind,0 , (int)(bufsize));
        assertBufferGood(in);
        in.seek(2*1024*1024+900);
        assertBufferGood(in);
        byte[] bytesjump= new byte[(int)(bufsize)];
        in.read(bytesjump,0,(int) (bufsize));
        assertBufferGood(in);

        assertTrue("expect accross buf backward seek true",Arrays.equals(bytes,bytesjump));



        in.seek(4*1024*1024-5000);
        in.read(bytesjump,0,(int) (bufsize));

        assertTrue("expect accross buf backward seek true",Arrays.equals(outbytes3,bytesjump));

        in.close();
        //fsDataInputStream.seek(4 * 1024 * 1024);


    }


    @Test
    public void testSpecifyData() throws Exception {
        testRootPath="/hbasetest002";
        Path smallSeekFile = setPath("/data/default/BTable/fd2c7a7443b4c90916ffd2a8052d1e23/family/fe971f04f1ba44abaea697ab00508751");
        //Path smallSeekFile = setPath("build_sds.txt");

        generateFile(this.fs, smallSeekFile, 100*1024*1024+6000, 256, 255);
        LOG.info("5MB file created: smallSeekFile.txt");


        long now= System.currentTimeMillis();
        FSDataInputStream fsDataInputStream = this.fs.open(smallSeekFile);
        OBSReadaheadInputStream in =
                (OBSReadaheadInputStream)fsDataInputStream.getWrappedStream();

        int size=91657518;
        byte[] bytesbuffer=new byte[size];


        int bufLen=33;
        int seekLen=0;
        RandomAccessFile file=new RandomAccessFile("d://605fb88421174214b80142f4a23c0825","r");
        file.seek(1898);
        file.seek(seekLen);
        InputStream fileStream = Channels.newInputStream(file.getChannel());



        in.seek(1898);
        in.seek(seekLen);
        byte[] buf = new byte[bufLen];
        byte[] fbuf = new byte[bufLen];
        long bytesRead = 0;
        while (bytesRead < size) {
            int bytes;
            int fbytes;
            if (size - bytesRead < bufLen) {
                int remaining = (int)(size - bytesRead);
                bytes = in.read(buf, 0, remaining);
                fbytes = fileStream.read(fbuf, 0, remaining);
                assertTrue(bytes==fbytes);
                assertTrue("buf: "+DatatypeConverter.printHexBinary(buf)+"\nfbuf: "+DatatypeConverter.printHexBinary(fbuf)
                        ,Arrays.equals(buf,fbuf));
            } else {
                bytes = in.read(buf, 0, 2);
                bytes += in.read(buf, 2, bufLen-2);
                fbytes = fileStream.read(fbuf, 0, 2);
                fbytes += fileStream.read(fbuf, 2, bufLen-2);
                assertTrue(bytes==fbytes);
                assertTrue("buf: "+DatatypeConverter.printHexBinary(buf)+"\nfbuf: "+DatatypeConverter.printHexBinary(fbuf)
                        ,Arrays.equals(buf,fbuf));
                ByteBuffer byteBuffer= ByteBuffer.wrap(buf);

                System.out.println(DatatypeConverter.printHexBinary(buf));

                return;
            }
            bytesRead += bytes;
            LOG.warn("bytes read: "+bytes);


            if (bytes<=0){
                break;
            }

            if (bytesRead % (1024 * 1024) == 0) {
                int available = in.available();
                int remaining = (int)(size - bytesRead-seekLen);
                assertTrue("expected remaining:" + remaining + ", but got:" + available,
                        remaining == available);
                LOG.info("Bytes read: " + Math.round((double)bytesRead / (1024 * 1024))
                        + " MB");
            }
        }

       // in.read(bytes,0, 91657518);

        FileUtils.writeByteArrayToFile(new File("testfilename"),bytesbuffer);


        fileStream.close();
        in.close();
        //fsDataInputStream.seek(4 * 1024 * 1024);


    }

    @Test
    public void testBloom() throws Exception {
        //testRootPath="/hbasetest002";
        Path smallSeekFile = setPath("/data/default/BTable/fd2c7a7443b4c90916ffd2a8052d1e23/family/fe971f04f1ba44abaea697ab00508751");
        //Path smallSeekFile = setPath("build_sds.txt");
        generateFile(this.fs, smallSeekFile, 100*1024*1024+6000, 556, 342);

        //generateFile(this.fs, smallSeekFile, 100*1024*1024+6000, 256, 255);
        LOG.info("5MB file created: smallSeekFile.txt");

        FSDataInputStream fsDataInputStream0 = this.fs.open(smallSeekFile);
        OBSReadaheadInputStream in0 =
                (OBSReadaheadInputStream)fsDataInputStream0.getWrappedStream();
        byte[] headbuf0=new byte[33];

        in0.seek(552304546);
        in0.read(headbuf0,0, (int) (33));
        ByteBuffer byteBuffer0= ByteBuffer.wrap(headbuf0);
        System.out.println(DatatypeConverter.printHexBinary(headbuf0));
        System.out.println(byteBuffer0.getInt(8));
        in0.close();

        long now= System.currentTimeMillis();
        FSDataInputStream fsDataInputStream = this.fs.open(smallSeekFile);
        OBSReadaheadInputStream in =
                (OBSReadaheadInputStream)fsDataInputStream.getWrappedStream();

        int size=33;
        byte[] headbuf=new byte[size];

        in.seek(61321633);
        byte[] bytes1=new byte[65651];
        in.read(bytes1,0,65651);

        in.seek(552304546);
        in.seek(552154213);

        byte[] bytes2= new byte[116514];
        in.read(bytes2,0,116514);
        in.seek(552304546);


        int bufLen=33;
        int seekLen=0;


        in.read(headbuf,0, (int) (size));
        ByteBuffer byteBuffer= ByteBuffer.wrap(headbuf);
        //300038F8F0D3014002480050D4AEA187025A2D6F72672E6170616368652E686164
        System.out.println(DatatypeConverter.printHexBinary(headbuf));


        //32780
        System.out.println(byteBuffer.getInt(8));

        in.close();



    }



    @Test
    public void testReadFile() throws Exception {
        final int bufLen = 256;
        final int sizeFlag = 1;
        String filename = "readTestFile_" + sizeFlag + ".txt";
        Path readTestFile = setPath("/test/" + filename);
        long size = sizeFlag * 1024 * 1024;

        generateFile(this.fs, readTestFile, size, 256, 255);
        LOG.info(sizeFlag + "MB file created: /test/" + filename);

        FSDataInputStream instream = this.fs.open(readTestFile);
        byte[] buf = new byte[bufLen];
        long bytesRead = 0;
        while (bytesRead < size) {
            int bytes;
            if (size - bytesRead < bufLen) {
                int remaining = (int)(size - bytesRead);
                bytes = instream.read(buf, 0, remaining);
            } else {
                bytes = instream.read(buf, 0, bufLen);
            }
            bytesRead += bytes;

            if (bytesRead % (1024 * 1024) == 0) {
                int available = instream.available();
                int remaining = (int)(size - bytesRead);
                assertTrue("expected remaining:" + remaining + ", but got:" + available,
                        remaining == available);
                LOG.info("Bytes read: " + Math.round((double)bytesRead / (1024 * 1024))
                        + " MB");
            }
        }
        assertTrue(instream.available() == 0);
        IOUtils.closeStream(instream);
    }
}
