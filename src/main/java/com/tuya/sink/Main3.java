package com.tuya.sink;

import com.alibaba.fastjson.JSON;
import com.tuya.mysqlDemo.Student;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;

/**
 *  @day 04
 * @author zhouxl
 * 接受Kafka消息
 */
public class Main3 {

    public static  void main(String[] args) throws Exception{

        final StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props =new Properties();

        props.put("bootstrap.servers","172.16.248.136:9092");


        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("auto.offset.reset","latest");

        SingleOutputStreamOperator<Student> student =env.addSource(
                new FlinkKafkaConsumer<>("flink_demo",
                        //String序列化
                        new SimpleStringSchema(),
                        props
                )
        ).setParallelism(1).map(string-> JSON.parseObject(string,Student.class));

        /**
         * 种类1
         * 将从kafka读到的数据打印在控制台(2个效果一样)
         */
//        student.print();
//        student.addSink(new PrintSinkFunction<>());

        /**
         * 种类2
         * 将kafka数据写到数据库
         */
        student.addSink(new SinkToMysql());

        /**
         * 种类3
         * 将kafka数据处理后打印
         */
//        SingleOutputStreamOperator<Student> map =  student.map(new MapFunction<Student, Student>() {
//            @Override
//            public Student map(Student student) throws Exception {
//                Student s1=new Student();
//                s1.id=student.id;
//                s1.name=student.name;
//                s1.password=student.password;
//                s1.age=student.age+5;
//                return s1;
//            }
//        });
//        map.print();
        /**
         * 种类4
         * FlatMap采用一条记录并输出n个记录
         */
//        SingleOutputStreamOperator<Student> flatMap =  student.flatMap(
//                new FlatMapFunction<Student, Student>() {
//                    @Override
//                    public void flatMap(Student value, Collector<Student> out) throws Exception {
//                        if(value.id%2 ==0){
//                            out.collect(value);
//                        }
//                    }
//                }
//        );
//        flatMap.print();

        /**
         * 种类5
         * filter函数根据条件判断结果
         */
//        SingleOutputStreamOperator<Student> filter =student.filter(
//                new FilterFunction<Student>() {
//                    @Override
//                    public boolean filter(Student value) throws Exception {
//                        if(value.id>95){
//                            return true;
//                        }
//                        return false;
//                    }
//                }
//        );
//        filter.print();
        /**
         * 种类6
         * KeyBy在逻辑上是基于key对流进行分区。在内部，他使用hash函数对流进行分区，它返回keyedDataStream数据流
         */
//        KeyedStream<Student,Integer> keyBy =student.keyBy(
//                new KeySelector<Student, Integer>() {
//                    @Override
//                    public Integer getKey(Student value) throws Exception {
//                        return value.age;
//                    }
//                }
//        );
//        keyBy.print();

        /**
         * 种类7
         * Reduce返回单个对结果值，并且reduce操作每处理一个元素总是创建一个新值。常见对方法有average,sum,min,max,count
         * 先将数据流进行keyby操作，然后将student对象对age做了一个求平均值对操作
         */
//      SingleOutputStreamOperator<Student> reduce= student.keyBy(
////               new KeySelector<Student, Integer>() {
////                   @Override
////                   public Integer getKey(Student value)throws Exception{
////                       return  value.age;
////                   }
////               }
////       ).reduce(new ReduceFunction<Student>() {
////          @Override
////          public Student reduce(Student value1, Student value2) throws Exception {
////              Student student1 =new Student();
////              student1.name =value1.name+value2.name;
////              student1.id=(value1.id+value2.id)/2;
////              student1.password=value1.password+value2.password;
////              student1.age=(value1.age+value2.age)/2;
////              return student1;
////          }
////      });
////      reduce.print();

        /**
         * 种类8
         * Fold 通过将最后一个文件夹流与当前记录组合来退出keyedStream,它会发回数据流
         */
//        KeyedStream.fold(
//                "1", new FoldFunction() {
//                    @Override
//                    public Object fold(String o, Integer value) throws Exception {
//                        return o+"="+value;
//                    }
//                }
//        );


        /**
         * 种类9
         * DataStream api支持各种聚合，类如min,max,sum等。这些函数可以应用于keyedStream以获得聚合
         * max返回流中最大值，maxBy返回具有最大值的健
         */
//         KeyedStream.max(0);
//         KeyedStream.sum("key");
//         KeyedStream.min("key");
//         KeyedStream.maxBy(0);
//         KeyedStream.maxBy("key");
//
//        env.execute("Flink 添加sink");

        /**
         * 种类10
         * Window函数允许按时间或者其他条件对现有对keyedStream进行分组。以下是以10s时间窗口聚合
         */
//        inputStream.keyBy(0).window(Time.seconds(10));

        /**
         * 种类11
         * WindowAll函数允许按时间或者其他条件对现有对keyedStream进行分组,通常，这是非并行数据转换，因为它在非分区数据流上运行
         */
//        inputStream.keyBy(0).windowAll(Time.seconds(10));

        /**
         * 种类12
         * union将2个或者多个数据流结合在一起
         */
//        inputStream.union(inputStream1,inputStream2,...);

        /**
         * 种类13
         * 通过一些key将同一个window的2个数据流join起来
         * 5s的窗口链接2个流，其中第一个流的第一个属性的连接条件等于另一个流的第二个属性
         */
//        inputStream.join(inputStream1).where(0).equalTo(1).window(Time.seconds(5)).apply(
//          new JoinFunction(){...}
//        );

        /**
         * 种类14
         * split 根据条件将流拆分成2个或者多个流，当你获得混合流并且你可能希望单独处理每个数据流时，可以使用此方法
         */
//        SplitStream<Integer>  split =inputStream.split(new OutputSelector<Integer>(){
//
//
//            @Override
//            public Iterable<String> select(Integer value) {
//                List<String> output =new ArrayList<String>();
//                if(value % 2==0){
//                    output.add("even");
//                }
//                else{
//                    output.add("odd");
//                }
//                return output;
//            }
//        });
        /**
         * 种类15
         * select 从拆分流中选择特定的流
         */
//        SplitStream<Integer> split;
//        DataStream<Integer> even = split.select("even");
//        DataStream<Integer> odd = split.select("odd");
//        DataStream<Integer> all = split.select("even","odd");

        /**
         * 种类16
         * Project函数允许你从事件流中选择属性子集，并仅将所选元素发送到下一个处理流
         * 给定记录中选择属性号 2 和 3
         */
//        DataStream<Tuple4<Integer,Double,String,String>> in;
//        DataStream<Tuple2<String,String>> out =in.project(3,2);



    }
}
