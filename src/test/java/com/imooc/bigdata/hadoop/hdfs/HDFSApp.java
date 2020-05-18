package com.imooc.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sun.nio.ch.IOUtil;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author EaApple
 * @2020/3/30 10:26
 * description：
 * java  api 操作hdfs
 *
 */

public class HDFSApp {
    public static final String HDFS_PATH = "hdfs://hadoop000:8020";
    public static final String USER_NAME ="hadoop";

    FileSystem fileSystem = null;
    Configuration configuration = null;


    @Before
    public void setUp() throws URISyntaxException, IOException, InterruptedException {
        configuration = new Configuration();
        // 重要的思路逻辑
        configuration.set("dfs.replication","1");
        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration,USER_NAME);
    }

    @After
    public void tearDown(){
        configuration = null;
        fileSystem = null;
    }

/*
* 创建hdfs文件夹
* */

    @Test
    public void mkdir() throws IOException {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /*
    * 查看文件内容
    * */
    @Test
    public void cat() throws IOException {
        FSDataInputStream fsDataInputStream =fileSystem.open(new Path("/hdfsapi/test"));
        IOUtils.copyBytes(fsDataInputStream,System.out,1024);
    }

    /*
    * 创建文件
    * */
    @Test
    public void create() throws IOException {
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/hdfsapi/test/b.txt"));
        fsDataOutputStream.writeUTF("create file test");
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }

    /*
    * 查看副本数
    * */
    @Test
    public void testReplication(){
        System.out.println(configuration.get("dfs.replication"));
    }


    /*
    * 重命名
    * */
    public void rename() throws IOException {
        Boolean result = fileSystem.rename(new Path("/hdfsapi/test/a.txt"),new Path("/hdfsapi/test/c.txt"));
        System.out.println(result);
    }
    /*
     * 上传小文件
     *
     * */
    @Test
    public void upLoadFile() throws IOException {
        fileSystem.copyFromLocalFile(new Path("H:/bigdata/upload.txt"),new Path("/hdfsapi/test/"));
    }
    /*
    拷贝大文件  使用文件流   带进度显示
    * */
    @Test
    public void copyLocalBigFile() throws IOException {
        InputStream inputStream = new BufferedInputStream(new FileInputStream((new File("E:/ScreenRecords/C1.mp4"))));
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/hdfsapi/test/C1.mp4"), new Progressable() {
            @Override
            public void progress() {
                System.out.println(".");
            }
        });
        IOUtils.copyBytes(inputStream,fsDataOutputStream,1024);
    }
    /*
    *  下载到本地
    * */
    @Test
    public void copyToLocalFile() throws IOException {
        fileSystem.copyToLocalFile(false,new Path("/hdfsapi/test/MyTest.txt"),new Path("H:/demo/demo.txt"),true);
    }

    /*
    列出所有文件
    * */
    @Test
    public void listFiles() throws IOException {
        FileStatus[] statuses = fileSystem.listStatus(new Path("/hdfsapi/test/"));
        for(FileStatus file : statuses) {
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long length = file.getLen();
            String path = file.getPath().toString();


            System.out.println(isDir + "\t" + permission
                    + "\t" + replication + "\t" + length
                    + "\t" + path
            );
        }
    }

    /*
    * 递归展示文件夹
    * */
    @Test
    public void listFilesRecursive() throws Exception {
        // listFiles   继承自FileStatus
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/hdfsapi/test"), true);

        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long length = file.getLen();
            String path = file.getPath().toString();


            System.out.println(isDir + "\t" + permission
                    + "\t" + replication + "\t" + length
                    + "\t" + path
            );
        }
    }

    /**
     * 查看文件块信息
     */
    @Test
    public void getFileBlockLocations() throws Exception {

        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfsapi/test/C1.mp4"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus,0,fileStatus.getLen());

        for(BlockLocation block : blocks) {

            for(String name: block.getNames()) {
                System.out.println(name +" : " + block.getOffset() + " : " + block.getLength() + " : " + block.getHosts());
            }
        }
    }

    /**
     * 删除文件
     */
    @Test
    public void delete() throws Exception {
        boolean result = fileSystem.delete(new Path("/hdfsapi/test/C1.mp4"), true);
        System.out.println(result);
    }

//    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
//        // hdfs编程入口
//        Configuration configuration = new Configuration();
//
//        // 此处已经映射了一个主机地址为
//        FileSystem fileSystem=FileSystem.get(new URI("hdfs://hadoop000:8020"),configuration,"hadoop");
//
//        Path path =new Path("/hdfsapi/text");
//        Boolean result = fileSystem.mkdirs(path);
//        System.out.println(result);
//    }
}

