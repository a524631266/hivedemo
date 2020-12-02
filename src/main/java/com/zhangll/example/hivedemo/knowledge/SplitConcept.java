package com.zhangll.example.hivedemo.knowledge;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

/**
 * mapred（旧少的） 与 mapreduce（新的）的区别
 * 使用这个来查看 ，根据任务的实际上下文对象 获取的实际的切片信息
 * 这样保证第一 不用加载数据
 * 第二，可以根据切片信息来并发执行数据信息
 * @see InputFormat#getSplits(JobContext) inputFormat是client端使用的用户接口，用来操作input数据
 * @see org.apache.hadoop.mapreduce.lib.input.FileSplit
 */
public class SplitConcept {
    public static void main(String[] args) throws IOException {
        // 获取hdfs的切片信息
        // 1. BlockLocation 是关于block的网络文件信息 块的文件信息，用来包
        // private String[] hosts; // Datanode hostnames
        //  private String[] cachedHosts; // Datanode hostnames with a cached replica
        //  private String[] names; // Datanode IP:xferPort for accessing the block
        //  private String[] topologyPaths; // Full path name in network topology
        //  private long offset;  // Offset of the block in the file
        //  private long length;
        //  private boolean corrupt;
        // 逻辑分区

    }

}
