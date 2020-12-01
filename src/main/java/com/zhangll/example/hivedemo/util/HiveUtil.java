package com.zhangll.example.hivedemo.util;


import java.sql.*;

/**
 * 可以直接运行查询
 */
public class HiveUtil {
    public static String driver="org.apache.hive.jdbc.HiveDriver";
    public static String url="jdbc:hive2://192.168.10.61:10000/ods";

    //注册驱动
    static{
        try{
            Class.forName(driver);
        }catch(ClassNotFoundException e){
            throw new ExceptionInInitializerError(e);
        }
    }
    //获取连接
    public static Connection getConnection(){
        try{
            return DriverManager.getConnection(url);
        }catch(SQLException e){
            e.printStackTrace();
        }
        return null;
    }
    //释放资源
    public static void release(Connection conn, Statement st, ResultSet rs){
        if(conn!=null){
            try{
                conn.close();
            }catch(SQLException e){
                e.printStackTrace();
            }finally{
                conn=null;
            }
        }

        if(st!=null){
            try{
                st.close();
            }catch(SQLException e){
                e.printStackTrace();
            }finally{
                st=null;
            }
        }

        if(rs!=null){
            try{
                rs.close();
            }catch(SQLException e){
                e.printStackTrace();
            }finally{
                rs=null;
            }
        }


    }
}
