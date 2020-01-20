package com.tuya.mysqlDemo;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author zhouxl
 * Source 类 SourceFromMySQL.java，该类继承 RichSourceFunction ，实现里面的 open、close、run、cancel 方法：
 */
public class SourceFromMySQL  extends RichSourceFunction<Student> {

    PreparedStatement ps;

    private Connection connection;

    /**
     * OPEN()方法中建立连接，这样不用每次invoke的时候都要建立连接和释放资源
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters)throws Exception{
        super.open(parameters);
        connection =getConnection();
        String sql ="select * from Student";
        ps =this.connection.prepareStatement(sql);
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
     * DataStream调用一次run方法来获取数据
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        ResultSet resultSet =ps.executeQuery();
        while(resultSet.next()){
            Student student =new Student(
                    resultSet.getInt("id"),
                    resultSet.getString("name").trim(),
                    resultSet.getString("password").trim(),
                    resultSet.getInt("age"));
            ctx.collect(student);
        }
    }

    /**
     * Cancels the source. Most sources will have a while loop inside the
     * {@link #run(SourceContext)} method. The implementation needs to ensure that the
     * source will break out of that loop after this method is called.
     *
     * <p>A typical pattern is to have an {@code "volatile boolean isRunning"} flag that is set to
     * {@code false} in this method. That flag is checked in the loop condition.
     *
     * <p>When a source is canceled, the executing thread will also be interrupted
     * (via {@link Thread#interrupt()}). The interruption happens strictly after this
     * method has been called, so any interruption handler can rely on the fact that
     * this method has completed. It is good practice to make any flags altered by
     * this method "volatile", in order to guarantee the visibility of the effects of
     * this method to any interruption handler.
     */
    @Override
    public void cancel() {

    }


    private static  Connection getConnection(){
        Connection con =null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://172.16.248.10:3306/flink_demo?characterEncoding=utf-8&useUnicode=true&autoReconnect=true&allowMultiQueries=true", "root", "root");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return con;


    }

}
