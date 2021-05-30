package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

/**
 * Author: Felix
 * Date: 2021/3/24
 * Desc: 独立访客（UV） 明细
 * 需要启动的进程以及应用
 * zk、kafka、logger.sh(nginx +采集服务)
 * BaseLogApp\UniqueVisitApp
 * 执行流程：
 * -运行模拟生成日志数据的jar
 * -数据发送给nginx
 * -nginx将数据的处理转发给202、203、204上的日志采集服务
 * -日志采集服务将数据发送到kafka的ods_base_log
 * -BaseLogApp从ods_base_log读取数据，进行分流
 * 根据日志类型，分别将数据发送到kafka的dwd_XXX_log
 * -UniqueVisitApp从kafka的dwd_page_log中读取数据
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //TODO 2.从kafka中读取数据
        //2.1 定义kafka主题以及消费者组
        String sourceTopic = "dwd_page_log";
        String groupId = "uniquevisitapp_group";
        String sinkTopic = "dwm_unique_visit";

        //2.2 获取Kafka消费者
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //2.3 对结构进行转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //jsonObjDS.print(">>>>");

        //TODO 3.按照mid对数据进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 4.对分组之后的数据进行过滤
        SingleOutputStreamOperator<JSONObject> filterDS = keyedDS.filter(
            new RichFilterFunction<JSONObject>() {
                //定义状态   保存设备首次访问时间
                ValueState<String> firstVisitDateValue;
                //定义格式化日期工具类
                SimpleDateFormat sdf;

                @Override
                public void open(Configuration parameters) throws Exception {
                    //给日期工具类赋值
                    sdf = new SimpleDateFormat("yyyyMMdd");
                    //创建状态描述器
                    ValueStateDescriptor<String> valueStateDescriptor =
                        new ValueStateDescriptor<String>("firstVisitDateValue", String.class);
                    //创建状态配置对象   设置状态有效时间为1天
                    StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build();
                    //设置有效时间为1day
                    valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                    //给状态进行赋值
                    firstVisitDateValue = getRuntimeContext().getState(valueStateDescriptor);
                }

                @Override
                public boolean filter(JSONObject jsonObj) throws Exception {
                    //将last_page_id不为空的过滤掉
                    String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                    if (lastPageId != null && lastPageId.length() > 0) {
                        return false;
                    }

                    //获取当前访问时间戳
                    Long ts = jsonObj.getLong("ts");
                    //将时间戳转换为日期
                    String curDate = sdf.format(ts);
                    //获取首次访问日期
                    String firstVisitDate = firstVisitDateValue.value();

                    if (firstVisitDate != null && firstVisitDate.length() > 0 && curDate.equals(firstVisitDate)) {
                        //说明曾经访问过
                        System.out.println("已访问过==>mid:" + jsonObj.getJSONObject("common").getString("mid") + "" +
                            ",firstVisitDate:" + firstVisitDate + ",curDate:" + curDate);
                        return false;
                    } else {
                        //说明没有访问
                        System.out.println("未访问过==>mid:" + jsonObj.getJSONObject("common").getString("mid") + "" +
                            ",firstVisitDate:" + firstVisitDate + ",curDate:" + curDate);
                        firstVisitDateValue.update(curDate);
                        return true;
                    }
                }
            }
        );

        filterDS.print(">>>>>");

        //TODO 5.将过滤之后的uv 写到kafka的dwm层
        //5.1 将jsonObj转换为String
        SingleOutputStreamOperator<String> resDS = filterDS.map(jsonObj->jsonObj.toJSONString());

        resDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        env.execute();
    }
}
