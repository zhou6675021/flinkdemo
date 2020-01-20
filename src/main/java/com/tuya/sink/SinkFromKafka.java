package com.tuya.sink;

import com.alibaba.fastjson.JSON;
import com.tuya.mysqlDemo.Student;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

/**
 *  @day 04
 * @author zhouxl
 */
public class SinkFromKafka {

    public static  final String broker_list ="172.16.248.136:9092,172.16.248.37:9092";

    public static  final String topic ="flink_demo";

    public static  void writeTokafak(Integer count) throws InterruptedException{

        //生产者配置文件，具体配置可参考ProducerConfig类源码，或者参考官网介绍
        Map<String,Object> config=new HashMap<String, Object>(16);
        //kafka服务器地址
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"172.16.248.136:9092");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.PARTITIONER_CLASS_CONFIG," org.apache.kafka.clients.producer.internals.DefaultPartitioner");

        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024*1024*5);


        KafkaProducer producer =new KafkaProducer<String,String>(config);

            Student student =new Student(count,"zhouxl"+count,"password",count);
            ProducerRecord record =new ProducerRecord<String,String>(topic,null,null,JSON.toJSONString(student));
            Future<RecordMetadata> future= producer.send(record);
            producer.flush();
            try{
                future.get();
                System.out.println("发送"+future.isDone()+"数据："+ JSON.toJSONString(student));
            }catch (Exception e){

            }

    }


    /**
     * 模拟kafka发送数据
     * @param args
     */
    public static void main(String[] args)throws InterruptedException{
        Integer count=1;
        while(true){
            Thread.sleep(5000);
            writeTokafak(count++);
        }

    }
}
