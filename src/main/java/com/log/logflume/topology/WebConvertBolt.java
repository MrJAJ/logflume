package com.log.logflume.topology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

/**
 * 日志数据预处理Bolt，实现功能：
 *     1.提取实现业务需求所需要的信息：ip地址、客户端唯一标识mid
 *     2.查询IP地址所属地，并发送到下一个Bolt
 */
public class WebConvertBolt extends BaseRichBolt {

    private OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        byte[] binary = input.getBinary(0);
        String s = new String(binary);
        try {
            String ip = s.substring(0, s.indexOf(" "));
            String time = s.substring(s.indexOf("[") + 1, s.lastIndexOf("]"));
            String log = s.substring(s.indexOf("\"") + 1, s.lastIndexOf("\""));
            String para = s.substring(s.lastIndexOf("\"") + 1).trim();

            System.out.println(ip + " " + time + " " + log + " " + para);
            // 发送数据到下一个bolt

            this.collector.emit(new Values(ip, time, log, para));
            collector.ack(input);
        }catch (Exception e){
            return;
        }
    }

    /**
     * 定义了发送到下一个bolt的数据
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ip", "time", "log", "para"));
    }
}
