package com.log.logflume.bolt;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.log.logflume.utils.JedisUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apdplat.word.analysis.CosineTextSimilarity;
import org.apdplat.word.analysis.EditDistanceTextSimilarity;
import org.apdplat.word.analysis.JaccardTextSimilarity;
import org.apdplat.word.analysis.TextSimilarity;
import redis.clients.jedis.Jedis;

import java.util.*;

public class SingleDBScanBolt extends BaseRichBolt {
    /**
     * kafkaSpout发送的字段名为bytes
     */
    private OutputCollector collector;
    private TextSimilarity co;
    private Set<String> unVisited;
    private Set<String> core;
    private Set<String> noise;
    double R;
    int MinPts;
    String m;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
        this.co = new CosineTextSimilarity();
        Jedis jedis = JedisUtil.getJedis();
        this.m=jedis.get("simcommethod");
        jedis.close();
        if(m!=null) {
            if (m.equals("1")) {
                this.co = new CosineTextSimilarity();
            } else if (m.equals("2")) {
                this.co = new JaccardTextSimilarity();
            } else if (m.equals("3")) {
                this.co = new EditDistanceTextSimilarity();
            }
        }else{
            m="1";
        }
        String r=jedis.get("R");
        String min=jedis.get("MinPts");

        R=r==null?2:Double.parseDouble(r);
        MinPts=min==null?10:Integer.parseInt(min);

        noise=new HashSet<>();

    }
    @Override
    public void execute(Tuple input) {
        String id = input.getStringByField("id");
        String model = input.getStringByField("model");
        List<String> params = JSONObject.parseArray(input.getStringByField("params"), String.class);
        //System.out.println("receive："+model+"\t"+R+"\t"+MinPts+"\t"+params.size()+"\t"+params);

        List<List<String>> cluster=dbscan(params,R,MinPts);
       // System.out.println("cluster："+cluster);
        for(List<String> c:cluster){
            if(c.size()<MinPts){
                noise.addAll(c);
                //System.out.println("sendglobal："+model+"\t"+noise.size()+"\t"+noise);
                this.collector.emit(new Values(id,model, JSON.toJSONString(noise)));
                collector.ack(input);
                noise.clear();
            }
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","model","params"));
    }

    public List<List<String>> dbscan(List<String> params,Double R,int MinPts){
        int k=0;
        unVisited=new HashSet<>(params);
        core=new HashSet<>();
        List<List<String>> cluster=new ArrayList<>();

        for(String d:params){
            List<String> t=new ArrayList<>();
            for(String i:params){
                if(distance(d,i)<=R){
                    t.add(i);
                }
                if(t.size()>=MinPts&&!core.contains(d)){
                    core.add(d);
                    break;
                }
            }
        }
        //System.out.println("core"+core);
        while (core.size()>0){
            Set<String> oldP=new HashSet<>(unVisited);
            Random random = new Random();
            List<String> tmp=new ArrayList<>(core);
            String o=tmp.get(random.nextInt(core.size()));
            unVisited.remove(o);
            List<String> Q=new ArrayList<>();
            Q.add(o);
            while(Q.size()>0){
                String q=Q.get(0);
                List<String> nq=new ArrayList<>();
                for(String i:params){
                    if(distance(q,i)<=R){
                        nq.add(i);
                    }
                }
                if(nq.size()>=MinPts){
                    Set<String>  Nq=new HashSet<>(nq);
                    Set<String> S = new HashSet<String>();
                    S.addAll(unVisited);
                    S.retainAll(Nq);
                    Q.addAll(S);
                    unVisited.removeAll(S);
                }
                Q.remove(q);
                //System.out.println("Q"+Q+Q.size());
            }
            k+=1;
            oldP.removeAll(unVisited);
            List<String> ck=new ArrayList<>(oldP);
            core.removeAll(ck);
            //System.out.println("cl"+ck+core.size()+core);
            cluster.add(ck);
        }
        return cluster;
    }

    public double distance(String p,String q){
        double distance=Double.MAX_VALUE;
        if(m.equals("1")||m.equals("2")){
            distance=(1-co.similarScore(p,q));
        }else if(m.equals("3")){
            int max=Math.max(p.length(),q.length());
            distance=(1-co.similarScore(p,q))*max;
        }
        return distance;
    }

}
