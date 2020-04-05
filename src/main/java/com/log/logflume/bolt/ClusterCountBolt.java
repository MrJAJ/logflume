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

public class ClusterCountBolt extends BaseRichBolt {
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
        String c = input.getStringByField("cluster");

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
        updateData(c,hourStr);
        updateData(c,dayStr);
        updateData("ClusterNum",c);
        Jedis jedis = JedisUtil.getJedis();
        Set<String> clu=jedis.smembers("Clusters");
        String oldLevelStr = jedis.hget("ClusterCate", c);
        if (oldLevelStr == null) {
            oldLevelStr = ""+id;
        }
        if(!oldLevelStr.contains(""+id)) {
            jedis.hset("ClusterCate", c, oldLevelStr + " " + id);
        }
        jedis.zincrby("ClusterRank:" + dayStr, 1, c);
        if(param.contains("ERROR")) {
            jedis.zincrby("ErrorClusterRank:" + dayStr, 1, c);
        }
        jedis.close();
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
