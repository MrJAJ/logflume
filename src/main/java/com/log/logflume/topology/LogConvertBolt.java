package com.log.logflume.topology;

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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * 日志数据预处理Bolt，实现功能：
 *     1.提取实现业务需求所需要的信息：ip地址、客户端唯一标识mid
 *     2.查询IP地址所属地，并发送到下一个Bolt
 */
public class LogConvertBolt extends BaseRichBolt {

    private OutputCollector collector;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH");
    SimpleDateFormat sdfDay = new SimpleDateFormat("yyyyMMdd");
    private final SimpleDateFormat webSdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
    private final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        String time = input.getStringByField("time");
        Date date=null;
        String dateStr="";
        String dayStr="";
        try {
            date=sdf2.parse(time);
            dateStr = sdf.format(date);
            dayStr = sdfDay.format(date);
        } catch (ParseException e) {
            e.printStackTrace();
            return;
        }
        String tempString = input.getStringByField("log");

        if(tempString.equals("")){ return;}
        tempString = tempString.replaceAll("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}：[0-9]{2}：[0-9]{2}(：[0-9]{3})?", "");
        if(input.getStringByField("logClass").startsWith("com.ism")) {
            tempString = tempString.replaceAll("[0-9a-zA-Z_-]+", "#");

        }
        tempString=tempString.trim();
        Jedis jedis = JedisUtil.getJedis();
//更新数量
        updateData(jedis,"LogNum",dayStr);
        updateData(jedis,"LogNum",dateStr);
        double score=0;
        CosineTextSimilarity cos=new CosineTextSimilarity();
        Set<String> clu=jedis.smembers("Clusters");
        for(String c:clu){
            score=cos.similarScore(tempString,c);
            if(score>0.85){
                updateData(jedis,c,dateStr);
                updateData(jedis,c,dayStr);
                updateData(jedis,"ClusterNum",c);

                String oldLevelStr = jedis.hget("ClusterCate", c);
                if (oldLevelStr == null) {
                    oldLevelStr = c;
                }
                if(!tempString.equals(c)&&!oldLevelStr.contains(tempString)) {
                    jedis.hset("ClusterCate", c, oldLevelStr + "\n" + tempString + "  " + score);
                }

                jedis.zincrby("ClusterRank:"+dayStr,1,c);
                jedis.close();
                this.collector.emit(new Values(tempString,1));
                collector.ack(input);
                return;
            }
        }
        jedis.sadd("Clusters",tempString);
        jedis.hset("ClusterNum",tempString,"1");
        jedis.hset("ClusterCate",tempString,tempString+" 1.0");
        jedis.hset(tempString, dateStr, "1");
        jedis.hset(tempString, dayStr, "1");
        jedis.zincrby("ClusterRank:"+dayStr,1,tempString);

        jedis.close();
        this.collector.emit(new Values(tempString,1));
        collector.ack(input);
    }

    /**
     * 定义了发送到下一个bolt的数据
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("cluster", "num"));
    }

    public synchronized void updateData(Jedis jedis,String key,String dateStr){
        String oldLevelStr = jedis.hget(key, dateStr);
        if (oldLevelStr == null) {
            oldLevelStr = "0";
        }
        int oldLevel = Integer.valueOf(oldLevelStr);
        jedis.hset(key, dateStr, oldLevel + 1 + "");
    }
}
