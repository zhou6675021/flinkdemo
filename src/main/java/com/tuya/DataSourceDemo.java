package com.tuya;





import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.base.Splitter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.SplittableIterator;
import scala.collection.Iterable;


/**
 * @author zhouxl
 * day02
 * 数据源的几种几种类型
 * 1.基于集合：有界数据集，更偏向本地测试
 * 2.基于文件，适合监听文件修改并读取内容
 * 3.基于socket:监听主机的host port,从socket中获取数据
 * 4.自定义addSource:无界
 */
public class DataSourceDemo {
    /**
     * 基于集合
     */
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    //1.集合

//      DataStreamSource<OUT>  input =env.fromCollection(Collection<OUT> data);

    //2.迭代器

   // DataStreamSource<OUT>  input =env.fromCollection(Iterable,Class);

    //3.给定的数据

   // DataStream<Out> input =env.fromElements();

    //4.从一个迭代器里创建并行数据流

//    DataStreamSource<OUT>  input = env.fromParallelCollection(SplittableIterator,Class)

    //5.创建一个生成制定区间范围内的数字序列的并行数据流
//    DataStreamSource<OUT>  input = env.generateSequence(from,to);

    /**
     * 基于文件
     */
//    DataStreamSource<String>  text = env.readFile(path);

    //6指定格式的文件输入格式读取文件

//    DataStreamSource<String>  text = env.readFile(fileInputFormat,path);

    //7这是上面两个方法内部调用的方法。它根据给定的 fileInputFormat 和读取路径读取文件。
    // 根据提供的 watchType，这个 source 可以定期（每隔 interval 毫秒）
    // 监测给定路径的新数据（FileProcessingMode.PROCESS_CONTINUOUSLY），
    // 或者处理一次路径对应文件的数据并退出（FileProcessingMode.PROCESS_ONCE）。
    // 你可以通过 pathFilter 进一步排除掉需要处理的文件

//    DataStreamSource<MyEvent>  stream = env.readFile(
//            myFormat,myFilePath, FileProcessingMode.PROCESS_CONTINUOUSLY,100,
//            FilePathFilter.createDefaultFilter(),typeInfo
//    );

    /**
     * 7的具体实现：
     * flink把文件读取过程分成2个子任务，即目录监控和数据读取。每个自任务都由单独的实体实现。目录监控由单个非并行（并行度为1）
     * 的任务执行，而数据读取由并行运行的多个任务执行。后者的并行性等于作业的并行性。单个目录监控任务的作用是扫描目录（根据watchType
     * 定期扫描或仅扫描一次），查找要处理的文件并把文件分割成切分片（splits）,然后将这些切分片分配给下游 reader.reader 负责
     * 读取数据。每个切分片只能由一个reader读取，但一个reader可以逐个读取多个切分片。
     *
     * 重要注意：
     *
     * 如果 watchType 设置为 FileProcessingMode.PROCESS_CONTINUOUSLY，则当文件被修改时，其内容将被重新处理。
     * 这会打破“exactly-once”语义，因为在文件末尾附加数据将导致其所有内容被重新处理。
     *
     * 如果 watchType 设置为 FileProcessingMode.PROCESS_ONCE，则 source 仅扫描路径一次然后退出，
     * 而不等待 reader 完成文件内容的读取。当然 reader 会继续阅读，直到读取所有的文件内容。
     * 关闭 source 后就不会再有检查点。这可能导致节点故障后的恢复速度较慢，因为该作业将从最后一个检查点恢复读取
     *
     */

    /**
     * 基于Socket
     * socketTextStream(String hostname, int port) - 从 socket 读取。元素可以用分隔符切分。
     */
    DataStream<Tuple2<String,Integer>> dataStream=env.socketTextStream("localhost",9999)
                                                     .flatMap(new SocketTextStreamWordCount.LinSplitter())
                                                     .keyBy(0)
            .timeWindow(Time.seconds(5))
            .sum(1);

    /**
     * 自定义
     * 添加新的数据源(类如kafka)
     */


//    DataStream<> dataStream=env.addSource(new FlinkKafkaConsumer011<>(...))

//    DataStream<KafkaEvent> input=env.addSource(
////            new FlinkKafkaConsumer<>(
////                    paramterTool.getRequired("input-topic"),//从参数中获取topic
////                    new KafkaDeserializationSchema(),
////                    paramterTool.getProperties())
////            .assignTimestampsAndWatermarks(new CustomWatermarkExtractor())
////    );






}
