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

import java.sql.Array;
import java.util.*;

public class ModelAnomyDetectionBolt extends BaseRichBolt {
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
        String uid=input.getStringByField("uid");
        String model = input.getStringByField("model");

        System.out.println(uid+"\t"+model);
        Jedis jedis = JedisUtil.getJedis();
        String lastModel=jedis.hget("lastModel",uid);
        Set<String> models=jedis.smembers("models");
        jedis.close();
        if(!models.contains(lastModel)){
            this.collector.emit(new Values(id,uid,lastModel,model));
            collector.ack(input);
        }
        Jedis jedis2 = JedisUtil.getJedis();
        String[] tmp=jedis2.hget("G",lastModel).split(" ");
        int[] GLastModel=new int[tmp.length];
        for(int i=0;i<tmp.length;i++){
            GLastModel[i]=Integer.parseInt(tmp[i]);
        }
        Arrays.sort(GLastModel);
        int num=jedis2.
        jedis2.close();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","uid","lastModel","model"));
    }

}
