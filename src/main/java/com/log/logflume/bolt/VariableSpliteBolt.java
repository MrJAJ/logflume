package com.log.logflume.bolt;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VariableSpliteBolt extends BaseRichBolt {
    /**
     * kafkaSpout发送的字段名为bytes
     */
    private OutputCollector collector;
    private Map<String,List<String>> maps;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
        this.maps=new HashMap<>();
    }
    @Override
    public void execute(Tuple input) {
        String id = input.getStringByField("id");
        String model = input.getStringByField("model");
        String param = input.getStringByField("param");
        if(param.isEmpty()){
            collector.ack(input);
            return;
        }
        List<String> tmp=maps.getOrDefault(model,new ArrayList<>());
        tmp.add(param);
        maps.put(model, tmp);
        if(maps.get(model).size()<100){
            collector.ack(input);
            //System.out.println(model+"\t"+maps.get(model).size());
        }else{
            String params=JSON.toJSONString(maps.get(model));
            this.collector.emit(new Values(id,model,params));
            //System.out.println(model+"\t"+params);
            collector.ack(input);
            maps.get(model).clear();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","model","params"));
    }

}
