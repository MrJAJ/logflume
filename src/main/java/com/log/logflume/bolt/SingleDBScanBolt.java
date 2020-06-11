package com.log.logflume.bolt;

import com.alibaba.fastjson.JSONObject;
import com.log.logflume.utils.JedisUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;

public class SingleDBScanBolt extends BaseRichBolt {
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
        List<String> params = JSONObject.parseArray(input.getStringByField("params"), String.class);
        Jedis jedis= JedisUtil.getJedis();
        float r=Float.parseFloat(jedis.get("R"));

        System.out.println("receive："+model+"\t"+params);
        this.collector.emit(new Values(id,model,params));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","model","param"));
    }

}
