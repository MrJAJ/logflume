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
        int model = input.getIntegerByField("model");

        //System.out.println(uid+"\t"+model);
        Jedis jedis = JedisUtil.getJedis();
        String last=jedis.hget("lastModel",uid);
        jedis.close();
        int lastModel=-1;
        if(last==null) {
            this.collector.emit(new Values(id, uid, lastModel, model));
            collector.ack(input);
            return;
        }

        lastModel=Integer.parseInt(last);

        Jedis jedis2 = JedisUtil.getJedis();
        String[] tmp=jedis2.hget("G",""+lastModel).split(" ");
        int[] GLastModel=new int[tmp.length];
        for(int i=0;i<tmp.length;i++){
            GLastModel[i]=Integer.parseInt(tmp[i]);
        }
        Arrays.sort(GLastModel);
        int num=Integer.parseInt(jedis2.hget("G",""+lastModel).split(" ")[model]);
        jedis2.close();
        if(num>=GLastModel[9]){
            this.collector.emit(new Values(id,uid,lastModel,model));
            collector.ack(input);
        }else{
            //System.out.println("Anomy"+uid+"\t"+lastModel+"\t"+model+"\t"+num);
            this.collector.emit(new Values(id,uid,lastModel,model,"3"));
            collector.fail(input);
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","uid","lastModel","model","type"));
    }

}
