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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;

public class CountBolt extends BaseRichBolt {
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

        //正则提取表达式
        String[] params=param.split("\\s");

        Jedis jedis = JedisUtil.getJedis();
        for(int i=0;i<params.length;i++){
            jedis.sadd("extras",params[i]);
            updateData(jedis,params[i],hourStr);
            updateData(jedis,params[i],dayStr);
        }
        System.out.println(id+"\t"+"message：\t"+message);
        Set<String> p=jedis.smembers("extras");
        for(String s:p){
            collector.emit(new Values( s,dayStr,jedis.hget(s,dayStr),hourStr,jedis.hget(s,hourStr)));
            collector.ack(input);
            System.out.println("Hbase save"+s+dayStr+"\t"+jedis.hget(s,dayStr)+"\t"+hourStr+"\t"+jedis.hget(s,hourStr));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("param", "dayStr","dayCount", "hourStr", "hourCount"));
    }
    public synchronized void updateData(Jedis jedis, String key, String dateStr){
        String oldLevelStr = jedis.hget(key, dateStr);
        if (oldLevelStr == null) {
            oldLevelStr = "0";
        }
        int oldLevel = Integer.valueOf(oldLevelStr);
        jedis.hset(key, dateStr, oldLevel + 1 + "");
    }
}
