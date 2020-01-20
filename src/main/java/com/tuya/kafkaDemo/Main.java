package com.tuya.kafkaDemo;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author zhouxl
 * 接受kafka消息
 */
public class Main {
    public static  void main(String[] args) throws Exception{

        final StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();

        Properties  props =new Properties();

        props.put("bootstrap.servers","172.16.248.136:9092");

//        props.put("zookeeper.connect","172.16.248.32:2181/kafka");

//        props.put("group.id","metric-group");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("auto.offset.reset","latest");

        DataStreamSource<String> dataStreamSource =env.addSource(
           new FlinkKafkaConsumer<String>("flink_demo",
                   //String序列化
                   new SimpleStringSchema(),
                   props
                   )
        ).setParallelism(1);

        //将从kafka读到的数据打印在控制台
        dataStreamSource.print();

        env.execute("Flink添加kafka资源");


    }

}
