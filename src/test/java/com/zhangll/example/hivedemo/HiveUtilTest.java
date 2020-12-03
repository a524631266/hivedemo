package com.zhangll.example.hivedemo;

import com.zhangll.example.hivedemo.util.HiveUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.hadoop.hive.HiveTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class HiveUtilTest {
//    String sql ="select rowkey, value from fault_stat_test limit 1";
    String sql ="select * from user3";
    @Resource
    private HiveTemplate hiveTemplate;

    @Before
    public void setUp() throws Exception {
    }

    /**
     * 测试成功
     */
    @Test
    public void test(){
        Connection conn=null;
        Statement st=null;
        ResultSet rs=null;
        try{
            //获取连接
            conn = HiveUtil.getConnection();
            //创建运行环境
            st=conn.createStatement();
            //运行HQL
            rs=st.executeQuery(sql);
            //处理数据
            while(rs.next()){
                String  name = rs.getString("rowkey");
                System.out.println(name);
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            HiveUtil.release(conn,st,rs);
        }
    }

    @Test
    public void testInsertIntoTable(){
        String sql = "insert into table user4 values ('001', '123456789','2010-10-10', '123')";
        Connection conn=null;
        Statement st=null;
        ResultSet rs=null;
        try{
            //获取连接
            conn = HiveUtil.getConnection();
            //创建运行环境
            st=conn.createStatement();
            //运行HQL
            rs=st.executeQuery(sql);
            //处理数据
            while(rs.next()){
                String  name = rs.getString("rowkey");
                System.out.println(name);
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            HiveUtil.release(conn,st,rs);
        }
    }

    @Test
    public void testHiveClient() throws SQLException {
//        TSocket tSocket = new TSocket("192.168.10.61", 10000);
//        tSocket.open();
//        TProtocol protocol = new TBinaryProtocol(tSocket);
//
//        HiveClient hiveClient = factory.getHiveClient();
        try {
//            List<String> execute = hiveClient.execute(sql);
//            for (String s : execute) {
//                System.out.println(s);
//            }
        }finally {
//            hiveClient.shutdown();
        }
    }

    /**
     * 仅仅查询第一列
     */
    @Test
    public void testQueryOne(){
        List<String> query = hiveTemplate.query(sql);
        for (String s : query) {
            System.out.println(s);
        }
    }

    /**
     * 仅仅查询第一列
     */
    @Test
    public void testQueryMore(){
        String execute = hiveTemplate.execute(callback -> {
            String s = callback.executeAndfetchOne(sql);
            System.out.println(s);
            return s;
        });
        System.out.println("同步查询");
    }

    /**
     * hive 聚合查询时很慢的，因此会启动一个大的任务进行批查询，所以
     * 数据统计是要使用hive作为一个批工具来实现聚合的。
     * select max(reg_date) from user3 group by mobile having mobile = 123456789;
     *
     * 在hive中，聚合会根据
     */
    @Test
    public void testQueryMore2(){
//        String execute = hiveTemplate.query("select sum() from user3");
    }



    @Test
    public void testInsertIntoHive(){
//        hiveTemplate.execute(hiveClient -> {
//            hiveClient.execute("insert into table ods.faultdetail(entityid, starttime, endtime, parts, datetype,day, feature) values (00000001, 1234567890000, 1234567890000, no, HH, 2018-10-11, PHS)");
//        });
    }


}