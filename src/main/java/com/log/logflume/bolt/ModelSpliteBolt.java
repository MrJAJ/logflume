package com.log.logflume.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class ModelSpliteBolt extends BaseRichBolt {
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
        String model = input.getStringByField("model");
        String param = input.getStringByField("param");
        System.out.println(model);
        String[] s=new String[]{"98809609262727168，","99558628322705408，","97638799831465984，","98365864436301824，"};
        int n= (int) (Math.random()*4);
        String uid=s[n];
        this.collector.emit(new Values(id,uid,model));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","uid","model"));
    }

}
