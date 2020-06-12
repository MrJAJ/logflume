package com.log.logflume.bolt;

import com.log.logflume.utils.JedisUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.analysis.CosineTextSimilarity;
import org.apdplat.word.analysis.EditDistanceTextSimilarity;
import org.apdplat.word.analysis.JaccardTextSimilarity;
import org.apdplat.word.analysis.TextSimilarity;
import org.apdplat.word.segmentation.Word;
import redis.clients.jedis.Jedis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SpliteSimBolt extends BaseRichBolt {
    /**
     * kafkaSpout发送的字段名为bytes
     */
    private OutputCollector collector;
    TextSimilarity co;
    SimpleDateFormat sdftime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    SimpleDateFormat sdfHour = new SimpleDateFormat("yyyyMMdd HH");
    SimpleDateFormat sdfDay = new SimpleDateFormat("yyyyMMdd");
    double clusterThresold;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
        Jedis jedis = JedisUtil.getJedis();
        String m=jedis.get("simcommethod");
        this.co = new CosineTextSimilarity();
        if(m!=null) {
            if (m.equals("1")) {
                this.co = new CosineTextSimilarity();
            } else if (m.equals("2")) {
                this.co = new JaccardTextSimilarity();
            } else if (m.equals("3")) {
                this.co = new EditDistanceTextSimilarity();
            }
        }
        clusterThresold = 0.5;
        String k=jedis.get("clusterThresold");
        jedis.close();
        if(k!=null){
            clusterThresold=Double.parseDouble(k);
        }


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
        double score=0;
        Jedis jedis1 = JedisUtil.getJedis();
        Set<String> clu=jedis1.smembers("Clusters");
        jedis1.close();
        for(String c:clu){
            score=com(message,c);
            if(score>clusterThresold){
                this.collector.emit(new Values(id,time,param,message,c,1));
                collector.ack(input);
                return;
            }
        }
        Jedis jedis = JedisUtil.getJedis();
        jedis.sadd("Clusters",message);
        String[] seq=listToArray(WordSegmenter.segWithStopWords(message));
        StringBuilder m=new StringBuilder();
        for(String tmp:seq){
            m.append(tmp+" ");
        }
        jedis.hset("ClusterModels",message,m.toString());
        jedis.hset("ClusterNum",message,"1");
        jedis.hset(message, hourStr, "1");
        jedis.hset(message, dayStr, "1");

        if(param.contains("ERROR")) {
            jedis.zincrby("ErrorClusterRank:" + dayStr, 1, message);
        }
        jedis.zincrby("ClusterRank:" + dayStr, 1, message);
        jedis.close();
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","time","param","message","cluster","count"));
    }

    public String[] listToArray(List<Word> list){
        String[] words=new String[list.size()];
        for(int i=0;i<list.size();i++){
            words[i]=list.get(i).getText();
        }
        return words;
    }

    public double com(String m,String n){
        return co.similarScore(m,n);
    }

}
