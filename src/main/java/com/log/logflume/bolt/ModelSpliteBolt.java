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
import java.util.Set;

public class ModelSpliteBolt extends BaseRichBolt {
    /**
     * kafkaSpout发送的字段名为bytes
     */
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
        Jedis jedis = JedisUtil.getJedis();
        for(int i=0;i<60;i++){
            String t="";
            for(int j=0;j<60;j++){
                t+=0+" ";
            }
            jedis.hset("G",""+i,t);
        }
        jedis.close();
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
        Jedis jedis = JedisUtil.getJedis();
        jedis.sadd("models",model);
        Set<String> models=jedis.smembers("models");
        jedis.close();
        int c=0;
        for(String t:models){
            if(t.equals(model)){
                break;
            }
            c++;
        }
        this.collector.emit(new Values(id,uid,c));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","uid","model"));
    }

}
