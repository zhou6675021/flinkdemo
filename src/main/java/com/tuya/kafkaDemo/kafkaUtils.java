package com.tuya.kafkaDemo;



import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @author zhouxl
 * kafka发送工具类
 */
public class kafkaUtils {

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


//        Properties props =new Properties();
//
//        props.put("bootstrap.servers",broker_list);
//        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer =new KafkaProducer<String,String>(config);

        Metric metric =new Metric();
        metric.setTimestamp(System.currentTimeMillis());
        metric.setName("mem");
//        Map<String,String> tags=new HashMap<>(16);
//        Map<String,Object> fields=new HashMap<>(16);
//
//        tags.put("cluster","zhouxl");
//        tags.put("host_ip","101.147.022.106");
//
//        fields.put("user_percent",90d);
//        fields.put("max",27244873d);
//        fields.put("used",17244873d);
//        fields.put("init",27244873d);
//
//
//        metric.setTags(tags);
//        metric.setFields(fields);

//        ProducerRecord record =new ProducerRecord<String,String>(topic,0,count.toString(), JSON.toJSONString(metric));
//        ProducerRecord record =new ProducerRecord<String,String>(topic,JSON.toJSONString(metric));
        ProducerRecord record =new ProducerRecord<String,String>(topic,metric.toString());


        Future<RecordMetadata> future= producer.send(record);
        producer.flush();
       try{
           future.get();
           System.out.println("发送"+future.isDone()+"数据："+JSON.toJSONString(metric));
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
            Thread.sleep(3000);
            writeTokafak(count++);
        }

    }

}
