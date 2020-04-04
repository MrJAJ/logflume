package com.log.logflume.bolt;

import com.log.logflume.utils.JedisUtil;
import com.log.logflume.utils.LCSMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;
import redis.clients.jedis.Jedis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ClusterSpellBolt extends BaseRichBolt {
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
        String c = input.getStringByField("cluster");


        Jedis jedis = JedisUtil.getJedis();
        String[] seq=listToArray(WordSegmenter.segWithStopWords(message));
        String[] model=getMatch(jedis,seq,c);
        if(model==null){
            jedis.hset("ClusterModels",c,seq.toString());
            jedis.hset("ModelIds",seq.toString(),""+id);
            jedis.hset("ModelParams",""+id,"");
        }else{
            insert(jedis,model,""+id,message,seq,c);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("param", "dayStr","dayCount", "hourStr", "hourCount"));
    }
    public String[] listToArray(List<Word> list){
        String[] words=new String[list.size()];
        for(int i=0;i<list.size();i++){
            words[i]=list.get(i).getText();
        }
        return words;
    }
    private String[] getMatch(Jedis jedis,String seq[],String c) {
        String[] bestMatch = null;
        int bestMatchLength = 0;

        //Find LCS of all existing LCSObjects and determine if they're a match as described in the paper
        for(String m : jedis.hget("ClusterModels",c).split(";")) {

            //Use the pruning described in the paper
            String[] model=m.split(" ");
            if(model.length < seq.length / 2 || model.length > seq.length * 2) {
                continue;
            }

            //Get LCS and see if it's a match
            int l =getLCS(model,seq);
            if(l >= seq.length / 2 && l > bestMatchLength) {
                bestMatchLength = l;
                bestMatch = model;
            }
        }

        return bestMatch;
    }
    public int getLCS(String[] model,String[] seq) {
        int count = 0;

        //Loop through current sequence using the simple loop approach described in the paper
        int lastMatch = -1;
        for(int i = 0; i < model.length; i++) {
            if(model[i].equals("*")) {
                continue;
            }

            for(int j = lastMatch + 1; j < seq.length; j++) {
                if(model[i].equals(seq[j])) {
                    lastMatch = j;
                    count++;
                    break;
                }
            }
        }

        return count;
    }
    public void insert(Jedis jedis,String[] model, String Id,String message,String[] seq,String c) {


        List<String> params=new ArrayList<>();
        String temp = "";

        //Create the new sequence by looping through it
        int lastMatch = -1;
        boolean placeholder = false; //Decides whether or not to add a * depending if there is already one preceding or not
        for(int i = 0; i < model.length; i++) {

            if(model[i].equals("*")) {
                if(!placeholder) {
                    temp = temp + "* ";
                }
                placeholder = true;
                continue;
            }
            for(int j = lastMatch + 1; j < seq.length; j++) {
                if(model[i].equals(seq[j])) {
                    StringBuilder b=new StringBuilder();
                    for(int k=lastMatch+1;k<j;k++) {
                        b.append(seq[k]);
                    }
                    if(b.length()>0) {
                        params.add(b.toString());
                    }
                    placeholder = false;
                    temp = temp + model[i] + " ";
                    lastMatch = j;
                    break;
                }else if(!placeholder) {
                    temp = temp + "* ";
                    placeholder = true;
                }
            }
        }
        if(lastMatch+1<seq.length){
            if(!placeholder) {
                temp = temp + "* ";
            }
            StringBuilder b=new StringBuilder();
            for(int k=lastMatch+1;k<seq.length;k++) {
                b.append(seq[k]);
            }
            if(b.length()>0) {
                params.add(b.toString());
            }
        }
        //Set sequence based of the common sequence found
        model = temp.trim().split("[\\s]+");
        String oldModel=jedis.hget("ClusterModels",c);

        jedis.hset("ClusterModels",c,oldModel+";"+model);
        String oldId=jedis.hget("ModelIds",message);
        jedis.hset("ModelIds",message,oldId+" "+Id);
        jedis.hset("ModelParams",""+Id,params.toString());
    }
}
