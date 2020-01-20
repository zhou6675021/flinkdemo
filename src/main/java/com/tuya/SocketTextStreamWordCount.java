package com.tuya;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhouxl
 * 实例demo
 * day01
 */
public class SocketTextStreamWordCount {
    public static void  main(String[] args) throws Exception{
      //参数检查
        if(args.length !=2){
            System.err.println("USAGE:\\nSocketTextStreamWordCount <hostname> <port>");
            return;
        }
       String hostname =args[0];
       Integer port =Integer.parseInt(args[1]);


       //设置流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      //获取数据
        DataStreamSource<String> stream  =env.socketTextStream(hostname,port);
      //计数
        SingleOutputStreamOperator<Tuple2<String,Integer>> sum =stream.flatMap(new LinSplitter())
                .keyBy(0)
                .sum(1);

        sum.print();

        env.execute("Java WordCount from SocketTextStream Example");
    }


    public static  final  class LinSplitter implements FlatMapFunction<String,Tuple2<String,Integer>>{


        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
           String[] tokens =s.toLowerCase().split("\\W+");

           for(String token :tokens){
               if(token.length()>0){
                   collector.collect(new Tuple2<String, Integer>(token,1));
               }
           }

        }
    }
}
