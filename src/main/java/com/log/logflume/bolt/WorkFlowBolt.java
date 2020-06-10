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

public class WorkFlowBolt extends BaseRichBolt {
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
        int model = input.getIntegerByField("model");
        int lastmodel = input.getIntegerByField("lastmodel");
        if(lastmodel==-1){
            Jedis jedis = JedisUtil.getJedis();
            jedis.sadd("models",""+model);
            jedis.close();
            collector.ack(input);
            return;
        }
        Jedis jedis = JedisUtil.getJedis();
        if(jedis.sismember("models",""+model)){
            String[] tmp=jedis.hget("G",""+lastmodel).split(" ");
            int n=Integer.parseInt(tmp[model]);
            tmp[model]=n+1+"";
            StringBuilder newtmp=new StringBuilder(tmp);

            jedis.hset("G",""+lastmodel,tmp.toString());
        }
        jedis.close();
        this.collector.emit(new Values(id,uid,model));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","uid","model"));
    }

}
