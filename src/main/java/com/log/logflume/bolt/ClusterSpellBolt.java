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
import org.apdplat.word.segmentation.Word;
import redis.clients.jedis.Jedis;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
        String id = input.getStringByField("id");
        String time = input.getStringByField("time");
        String param = input.getStringByField("param");
        String message = input.getStringByField("message");
        String c = input.getStringByField("cluster");

        String[] seq=listToArray(WordSegmenter.segWithStopWords(message));
        StringBuilder m=new StringBuilder();
        for(String tmp:seq){
            m.append(tmp+" ");
        }
        String[] model=getMatch(seq,c);
        if(model==null){
            Jedis jedis = JedisUtil.getJedis();
            String oldModels=jedis.hget("ClusterModels",c);
            oldModels=oldModels==null?"":oldModels+";";
            jedis.hset("ClusterModels",c,oldModels+m.toString());
            //jedis.hset("ModelIds",m.toString(),""+id+" ");
            jedis.close();
            collector.emit(new Values( id,c,model,""));
            collector.ack(input);
        }else{
            String[] np=insert(model,""+id,message,seq,c);
            collector.emit(new Values( id,c,np[0],np[1]));
            collector.ack(input);
            //System.out.println("save"+id+"\t"+c+"\t"+np[0]+"\t"+np[1]);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "cluster","model", "param"));
    }
    public String[] listToArray(List<Word> list){
        String[] words=new String[list.size()];
        for(int i=0;i<list.size();i++){
            words[i]=list.get(i).getText();
        }
        return words;
    }
    private String[] getMatch(String seq[],String c) {
        String[] bestMatch = null;
        int bestMatchLength = 0;

        //Find LCS of all existing LCSObjects and determine if they're a match as described in the paper
        Jedis jedis = JedisUtil.getJedis();
        String tmp=jedis.hget("ClusterModels",c);
        jedis.close();
        if(tmp==null){return bestMatch;}
        for(String m : tmp.split(";")) {

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
    public String[] insert(String[] model, String Id,String message,String[] seq,String c) {


        List<String> params=new ArrayList<>();
        String temp = "";
        StringBuilder oldm=new StringBuilder();
        for(String tmp:model){
            oldm.append(tmp+" ");
        }
        String[] result=new String[2];
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
        result[0]=temp.trim();
        model = temp.trim().split("[\\s]+");
        Jedis jedis = JedisUtil.getJedis();
        String oldModel=jedis.hget("ClusterModels",c);
        StringBuilder m=new StringBuilder();
        for(String tmp:model){
            m.append(tmp+" ");
        }
        //String oldId=jedis.hget("ModelIds",oldm.toString());
        String newModel=m.toString();
        if(!oldModel.equals(m.toString())){
            //jedis.hdel("ModelIds", oldm.toString());
            newModel=oldModel.replace(oldm.toString(),m.toString());
        }
        jedis.hset("ClusterModels",c,newModel);

        //jedis.hset("ModelIds",newModel.toString(),oldId+Id+" ");
        jedis.close();
        result[1]=params.size()==0?"":(params.toString()+" ");
        return result;
    }
}
