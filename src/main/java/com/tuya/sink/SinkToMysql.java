package com.tuya.sink;

import com.tuya.mysqlDemo.Student;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.apache.flink.configuration.Configuration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 *  @day 04
 * @author zhouxl
 * flink流的去向的处理
 */
public class SinkToMysql extends RichSinkFunction<Student> {

    PreparedStatement ps;

    private Connection connection;

    @Override
    public void open(Configuration  parameters) throws Exception{
        super.open(parameters);
        connection =getConnection();
        String sql ="insert into Student(id,name,password,age) values(?,?,?,?); ";
        ps=this.connection.prepareStatement(sql);
    }


    /**
     * 程序执行完毕，关闭链接和释放资源的动作了
     * @throws Exception
     */
    @Override
    public void close() throws Exception{
        super.close();
        if(connection !=null){
            connection.close();
        }
        if(ps!=null){
            ps.close();
        }
    }


    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(Student value, Context context) throws Exception {
        //组装数据，执行插入操作
        ps.setInt(1, value.getId());
        ps.setString(2, value.getName());
        ps.setString(3, value.getPassword());
        ps.setInt(4, value.getAge());
        ps.executeUpdate();
    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://172.16.248.10:3306/flink_demo?characterEncoding=utf-8&useUnicode=true&autoReconnect=true&allowMultiQueries=true", "root", "root");
        } catch (Exception e) {
            System.out.println("----------mysql链接异常 , msg = "+ e.getMessage());
        }
        return con;
    }

}
