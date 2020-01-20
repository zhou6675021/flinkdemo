package com.tuya.windows;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 *
 * @day 05
 * @author zhouxl
 * 流处理的窗口
 * 共有这几种：Time，Count,Session,api定制化
 */
public class StreamWindows {

    /**
     * tumbling time windows(翻滚时间窗口)
     * 时间参数
     */
//   public void   time(){
//       SingleOutputStreamOperator data1;
//
//       data1.keyBy(1)
//               //每分钟统计一次
//               .timeWindow(Time.minutes(1))
//               .sum(1);
//   }

    /**
     * sliding time windows(滑动时间窗口)
     * 2个时间参数
     */
//    public void   twotime(){
//        SingleOutputStreamOperator data1;
//
//        data1.keyBy(1)
//                //每隔30分钟统计过去一分钟的数量和
//                .timeWindow(Time.minutes(1),Time.seconds(30))
//                .sum(1);
//    }

    /**
     * 计数统计
     * 2种
     */
//   public WindowedStream<T,KEY, GlobalWindow> countWindow(long size){
//       return window(GlobalWindow.create()).trigger(PurgingTrigger.of(CountTrigger.of(size)));
//   }
//
//    public WindowedStream<T,KEY, GlobalWindow> countWindow(long size,long slide){
//        return window(GlobalWindow.create())
//                .evictor(CountEvictor.of(size))
//                .trigger(CountTrigger.of(slide));
//    }


//    public void   count(){
//        SingleOutputStreamOperator data1;
//
//        data1.keyBy(1)
//                //统计每100个元素的数量之和
//                .countWindow(100)
//                .sum(1);
//        }
//
//    public void   twocount(){
//        SingleOutputStreamOperator data1;
//
//        data1.keyBy(1)
//                //每10个元素统计过去100个元素的数量之和
//                .countWindow(100,10)
//                .sum(1);
//    }


    /**
     * 1。windows Assigner
     * 负责将元素分配到不同的window
     * window api提供自定义的windowAssigner接口，我们需要实现它
     * public abstract Collection(w) assignWindows(T element,long timestamp)
     * 同时，对于基于count的window而言，默认采用了GlobalWindow的window assigner,如下：
     *  keyBy.window(GlobalWindows.create())
     */

    /**
     * 2Trigger
     * 触发器，定义合适或什么情况下移除window
     * 我们可以指定触发器来覆盖windowAssigner提供的默认触发器。请注意，指定的触发器不会添加其他触发条件，但会替换当前的触发器
     */

    /**
     * 3 Evictor
     * 驱逐者，即保留上一window留下的某些元素
     */

    /**
     * 4，通过apply windowFunction来返回DataSteam 类型数据
     * 利用Flink 的内部窗口机制和DataStream API 可以实现自定义的窗口逻辑，类如 session window
     */

}
