package com.zhangll.example.hivedemo.knowledge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;

import java.io.IOException;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

public class HadoopFileSystemStatus {
    public static void main(String[] args) throws IOException {
        // 获取hdfs的系统信息
        Configuration config = new Configuration();
        config.set(FS_DEFAULT_NAME_KEY, "hdfs://192.168.10.61:8020/user/hive/warehouse/ods.db/user3");
        FileSystem fileSystem = FileSystem.get(config);
        FsStatus status = fileSystem.getStatus();
        System.out.println(status);
    }
}
