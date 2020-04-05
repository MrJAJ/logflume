package com.log.logflume.bolt;

import com.log.logflume.utils.JedisUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apdplat.word.analysis.CosineTextSimilarity;
import redis.clients.jedis.Jedis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;

public class SpliteSimBolt extends BaseRichBolt {
    /**
     * kafkaSpout发送的字段名为bytes
     */
    private OutputCollector collector;
    SimpleDateFormat sdftime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    SimpleDateFormat sdfHour = new SimpleDateFormat("yyyyMMdd HH");
    SimpleDateFormat sdfDay = new SimpleDateFormat("yyyyMMdd");

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
    }
    @Override
    public void execute(Tuple input) {
        long id = input.getLongByField("id");
        String time = input.getStringByField("time");
        String param = input.getStringByField("param");
        String message = input.getStringByField("message");

        Date date=null;
        String hourStr="";
        String dayStr="";
        try {
            date=sdftime.parse(time);
            hourStr = sdfHour.format(date);
            dayStr = sdfDay.format(date);
        } catch (ParseException e) {
            e.printStackTrace();
            return;
        }

        Jedis jedis = JedisUtil.getJedis();
        double score=0;
        CosineTextSimilarity cos=new CosineTextSimilarity();
        Set<String> clu=jedis.smembers("Clusters");
        for(String c:clu){
            score=cos.similarScore(message,c);
            if(score>0.5){
                this.collector.emit(new Values(id,time,param,message,c,1));
                collector.ack(input);
                return;
            }
        }
        jedis.sadd("Clusters",message);
        jedis.hset("ClusterNum",message,"1");
        jedis.hset("ClusterCate",message,""+id);
        jedis.hset(message, hourStr, "1");
        jedis.hset(message, dayStr, "1");

        if(param.contains("ERROR")) {
            jedis.zincrby("ErrorClusterRank:" + dayStr, 1, message);
        }
        jedis.zincrby("ClusterRank:" + dayStr, 1, message);
        jedis.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","time","param","message","cluster","count"));
    }

}
