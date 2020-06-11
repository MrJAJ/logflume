package com.log.logflume.bolt;

import com.log.logflume.utils.JedisUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class AnomyAlarmBolt extends BaseRichBolt {
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

        String type = input.getStringByField("type");
        String content="";
        if(type.equals("3")){
            String uid=input.getStringByField("uid");
            content=uid+"\t"+input.getStringByField("model");
        }else if(type.equals("4")){
            content=input.getStringByField("model")+"\t"+input.getStringByField("param");
        }
        System.out.println(content);
        Jedis jedis = JedisUtil.getJedis();
        jedis.hset("Anomy",type+"\t"+id,content);
        jedis.close();
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields());
    }

}
