package com.log.logflume.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class ReplaceBolt extends BaseRichBolt {
    /**
     * kafkaSpout发送的字段名为bytes
     */
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
    }
    @Override
    public void execute(Tuple input) {
        String id = input.getStringByField("id");
        String time = input.getStringByField("time");
        String param = input.getStringByField("param");
        String message = input.getStringByField("message");
        String log = input.getStringByField("log");

        //正则提取表达式
        String timeRex="(\\d+-\\d+-\\d+\\s\\d+：\\d+：\\d+：\\d+)\\s";
        String timeRex2="(\\d+-\\d+-\\d+\\s\\d+：\\d+：\\d+)";
        String timeRex3="(\\d+-\\d+-\\d+\\s\\d+\\d+:\\d+:\\d+)\\s";
        String timeRex4="(\\d+-\\d+-\\d+\\s\\d+:\\d+:\\d+)";

        String[] rexs=new String[]{timeRex,timeRex2,timeRex3,timeRex4};

        for(int i=0;i<rexs.length;i++) {
            message=message.replaceAll(rexs[i],"");
        }
        //System.out.println(id+"\t"+"message："+message);
        collector.emit(new Values( id,time,param,message,log));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","time","param", "message","log"));
    }

}
