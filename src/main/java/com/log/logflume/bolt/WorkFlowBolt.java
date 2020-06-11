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
        String uid = input.getStringByField("uid");
        int model = input.getIntegerByField("model");
        int lastmodel = input.getIntegerByField("lastModel");
        if(lastmodel==-1){
            Jedis jedis = JedisUtil.getJedis();
            jedis.hset("lastModel",uid,""+model);
            System.out.println("firstupdate"+"\t"+uid+"\t"+model);
            jedis.close();
            collector.ack(input);
            return;
        }
        Jedis jedis = JedisUtil.getJedis();
        String[] tmp=jedis.hget("G",""+lastmodel).split(" ");
        int n=Integer.parseInt(tmp[model]);
        //if(jedis.sismember("models",""+model)){
            tmp[model]=n+1+"";
            //jedis.sadd("models",""+model);
      //  }else{
          //  tmp[model]=1+"";
       // }
        StringBuffer newtmp=new StringBuffer();
        for(String t:tmp){
            newtmp.append(t+" ");
        }
        jedis.hset("G",""+lastmodel, newtmp.toString().trim());
        jedis.hset("lastModel",uid,""+model);
        System.out.println("update"+"\t"+lastmodel+"\t"+newtmp.toString().trim());
        System.out.println("update"+"\t"+uid+"\t"+model);
        jedis.close();
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","uid","model"));
    }

}
