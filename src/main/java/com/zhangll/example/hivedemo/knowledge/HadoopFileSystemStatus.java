package com.zhangll.example.hivedemo.knowledge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

/**
 * 远程方式调用用户的查看信息
 * 1. 获取 filesystem
 * 2. 根据fs获取 fsstate
 * 3. 转化为fs -》 DistributedFileSystem
 * 4. 获取子节点dataNodeStats
 * 5.
 */
public class HadoopFileSystemStatus {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        // 获取hdfs的系统信息
        Configuration config = new Configuration();
        config.set(FS_DEFAULT_NAME_KEY, "hdfs://192.168.10.61:8020/user/hive/warehouse/ods.db/user3");
        // 设置用户信息
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.10.61:8020/"),config, "hadoop");
        FsStatus status = fileSystem.getStatus();
        System.out.println(status);

        DistributedFileSystem hdfs = (DistributedFileSystem) fileSystem;
        // 获取所有系统的信息
        long capacity = hdfs.getDiskStatus().getCapacity() / 1024 /1024 /1024;
        long remain = hdfs.getDiskStatus().getRemaining() / 1024 /1024 /1024;

        // 获取hdfs的data节点状态信息
        // 1. capacity 容量信息
        // 2.  NetworkLocation : /default-rack
        DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
        for (int i = 0; i < dataNodeStats.length; i++) {
            // 7446 * 1024 * 1024 * 1024(7T数据)
            // 打印详情信息
            System.out.println(dataNodeStats[i].getDatanodeReport());
            System.out.println("HostName: " + dataNodeStats[i].getHostName());
            // 7995466465280
            System.out.println("Capacity: "+ dataNodeStats[i].getCapacity());
            System.out.println("NetworkLocation: " + dataNodeStats[i].getNetworkLocation());
            // null
            System.out.println("SoftwareVersion: " + dataNodeStats[i].getSoftwareVersion());
            // 0
            System.out.println("Level: "+ dataNodeStats[i].getLevel());
            // 2000 * 1024 * 1024 * 1024(2T数据)
            System.out.println("getRemaining: "+ dataNodeStats[i].getRemaining());
            // 7994847763666
            assert dataNodeStats[i].getDfsUsed() + dataNodeStats[i].getRemaining() + dataNodeStats[i].getNonDfsUsed() ==  dataNodeStats[i].getCapacity();
        }


        //
        FsStatus status1 = hdfs.getStatus(new Path("/user/hive/warehouse/ods.db/user3"));
        System.out.println(status1);
        ContentSummary contentSummary = hdfs.getContentSummary(new Path("/user/hive/warehouse/ods.db/user3"));
        // 这个是实际的存内容大小
        long length = contentSummary.getLength();
        // 这个是块大小，被分配的固定大小，可以往里面不断添加存储内容！！
        long defaultBlockSize = hdfs.getDefaultBlockSize();


        // 获取fileStatus
        getFileStatus(fileSystem);

    }

    private static void getFileStatus(FileSystem fileSystem) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/user/hive/warehouse/ods.db/user3/dt=123"));

        for (FileStatus fileStatus : fileStatuses) {
            System.out.println("getLen: " + fileStatus.getLen());
            System.out.println("BlockSize:  " + fileStatus.getBlockSize());
            System.out.println("Path : " + fileStatus.getPath());
            System.out.println("Permission : " + fileStatus.getPermission());
            System.out.println("AccessTime : " + fileStatus.getAccessTime());
            System.out.println("Replication : " + fileStatus.getReplication());
            System.out.println("Group : " + fileStatus.getGroup());
            // 如果不是symlink那么就直接报错
//            System.out.println("Symlink : " + fileStatus.getSymlink());
        }
    }
}
