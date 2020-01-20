package com.tuya.es;

import com.alibaba.fastjson.JSON;
import com.tuya.es.bean.MetricEvent;
import com.tuya.es.utils.ExecutionEnvUtil;
import com.tuya.es.utils.KafkaConfigUtil;
import com.tuya.kafkaDemo.Metric;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.http.HttpHost;

import org.elasticsearch.client.Requests;

import java.util.List;

import static com.tuya.es.utils.PropertiesConstants.*;

/**
 * @author zhouxl
 * es启动类
 */
public class Main {

    public static  void main(String[] args)throws Exception{
        //获取所有参数
        final ParameterTool parameterTool = ExecutionEnvUtil.createParamterTool(args);
        //准备好环境
        StreamExecutionEnvironment env =ExecutionEnvUtil.prepare(parameterTool);
        //从kafka读取数据
        DataStreamSource<MetricEvent>  data = KafkaConfigUtil.buildSource(env);
        //从配置文件中读取es的地址
        List<HttpHost>  esAddresses =ElasticSearchSinkUtil.getEsAddresses(parameterTool.get(ELASTICSEARCH_HOSTS));
        //从配置文件中读取bulk flush size,代表一次批处理的数量，这个可是性能调优参数，特别提醒
        int bulkSize =parameterTool.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS,40);
        //从配置文件中读取并行sink 数，这个也是性能调优参数，特别提醒，这样才能够更快的消费，防止kafka数据堆积
        int sinkParallelism =parameterTool.getInt(STREAM_SINK_PARALLELISM,5);

        //自己再自带的es sink上一层封装了下
        ElasticSearchSinkUtil.addSink(esAddresses,bulkSize,sinkParallelism,data,
                (MetricEvent metric, RuntimeContext runtimeContext, RequestIndexer requestIndexer)->{
                     requestIndexer.add(Requests.indexRequest()
                             .index(ZHOUXL+"_"+metric.getName()) //es索引名
                             .type(ZHOUXL) //ES TYPE
                             .source(JSON.toJSONBytes(metric))
                     );

                });
            env.execute("flink learning connectors es6");

    }
}
