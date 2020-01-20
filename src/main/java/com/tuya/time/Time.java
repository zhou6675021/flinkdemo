package com.tuya.time;

import org.apache.flink.streaming.api.TimeCharacteristic;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



/**
 * @day 06
 * @author zhouxl
 *
 * 几种不同的Time概念：Processing Time,Event Time和Ingestion Time
 */
public class Time {

    /**
     * 1Processing Time是指事件被处理时机器的系统时间
     * 当流程序在processing time上运行时，所有基于时间的操作(如时间窗口)将使用当时机器当系统时间。每小时 processing time
     * 窗口将包括在系统时钟指示整个小时之间到达特定操作的所有事件（如应用程序在上午9：15开始，则第一个每小时processing time
     * 窗口将包括在上午9：15到上午10：00之间处理的事件，下个窗口将上午10：00到11：00之间处理的事件）
     *
     * processing time 是最简单的time概念，不需要流和机器之间的协调，它提供了最好的性能和最低的延迟。但是，在分布式和异步的环境下
     * processing time不能提供确定性，因为它容易索道事件到达系统的速度（类如从消息队列），事件在系统内操作流动的速度以及中断的影响
     */

    /**
     * 2 Event time是事件发生的时间，一般就是数据本身携带的时间。这个时间通常是在事件到达flink之前将确定了，并且可以从每个事件中获取到事件
     * 时间戳。在event time中，时间取决于数据，而跟其他没什么关系。event time程序必须指定如何生成event time水印，这是表示event time进度
     * 的机制：完美的说，无论事件什么时候到达或者怎么排序，最后处理event time 将产生完全一致和确定的结果。但是，除非事件按照已知顺序（按事件的事件）
     * 到达，否则处理event time时将会因为要等待一些无序事件而产生一些延迟。由于只能等待一段有限的时间，因此就难以保证处理event time
     * 将产生一致和确定的结果
     * 注意：有时当event time程序实时处理数据时，他们将使用一些processing time操作，以确保他们及时进行
     */

    /***
     * Ingestion Time 是事件进入Flink的时间。在源操作处，每个时间将源的当前时间作为时间戳，并且基于时间的操作（如时间窗口）会利用这个时间戳。
     * 概念位于前2者之间，因为ingestion time使用稳定的时间戳（在源处分配一次），所以对事件对不同窗口操作将引用相同对时间戳，而在processing TIME
     * 每个窗口操作符可以将事件分配给不同对窗口（基于机器系统时间和到达延迟），与event time相比，ingestion time程序无法处理任何无序事件
     * 或者延迟数据，但程序不必指定如何生成水印。
     * 在flink中，ingestion time与Event time 非常相似，但ingestion time具有自动分配时间戳和自动生成水印功能
     */

    /**
     * flink dataStream程序的第一部分通常是设置基本时间特性。该设置定义了数据流源的行为方式（类如：他们是否将分配时间戳），以及像
     * keyedStream.timeWindow(Time.seconds(30))这样的窗口操作应该使用上面哪种时间概念
     */

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //其他
//        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        DataStream<MyEvent>  stream =env.addSource(new FlinkKafkaConsumer<MyEvent>(topic,schema,props));
//
//        stream.keyBy((event)->event.getUser())
//                .timeWindow(org.apache.flink.streaming.api.windowing.time.Time.minutes(1))
//                .reduce((a,b)->a.add(b))
//                .addSink(...);

    }

    /**
     * 由于event time 需要几s来处理数据，所以当前的event time 微微落后于processing time
     *
     * 衡量event time进度的机制是watermarks，watermark作为数据流的一部分流动并带有时间戳t，watermark(t) 声明Event Time已到达该
     * 流中的时间t，这意味着流中不应该具有时间戳t<=t的元素（即大于或等于水印的事件）
     */

}



