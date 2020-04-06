package com.log.logflume.bolt;
import com.log.logflume.utils.JedisUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class ExtraCountBolt extends BaseRichBolt {
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
        String id = input.getStringByField("id");
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


//日志量变化
        updateData("LogNum",dayStr);
        updateData("LogNum",hourStr);

        for(int i=0;i<params.length;i++){
            Jedis jedis = JedisUtil.getJedis();
            jedis.sadd("extras","params"+i);
            jedis.sadd("params"+i,params[i]);
            jedis.close();
            updateData(params[i],hourStr);
            updateData(params[i],dayStr);
        }
        //System.out.println(id+"\t"+"message：\t"+message);
//        Set<String> ex=jedis.smembers("extras");
//        for(String pa:ex){
//            Set<String> p=jedis.smembers(pa);
//            for(String s:p) {
//                collector.emit(new Values(s, dayStr, jedis.hget(s, dayStr), hourStr, jedis.hget(s, hourStr)));
//                collector.ack(input);
//                System.out.println("Hbase save!\t" + s + dayStr + "\t" + jedis.hget(s, dayStr) + "\t" + hourStr + "\t" + jedis.hget(s, hourStr));
//            }
//        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("param", "dayStr","dayCount", "hourStr", "hourCount"));
    }
    public synchronized void updateData(String key, String dateStr){
        Jedis jedis = JedisUtil.getJedis();
        String oldLevelStr = jedis.hget(key, dateStr);
        if (oldLevelStr == null) {
            oldLevelStr = "0";
        }
        int oldLevel = Integer.valueOf(oldLevelStr);
        jedis.hset(key, dateStr, oldLevel + 1 + "");
        jedis.close();
    }
}
