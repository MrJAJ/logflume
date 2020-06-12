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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExtractBolt extends BaseRichBolt {
    /**
     * kafkaSpout发送的字段名为bytes
     */
    private OutputCollector collector;
    IdGenerator idGenerator;
    String timeRex="(\\d+-\\d+-\\d+\\s\\d+:\\d+:\\d+.\\d+)";
    String extraRex="(\\S+)\\s\\d+\\s---\\s\\[([^\\]]*)\\]\\s+(\\S+)";
    String messageRex=":\\s(.*)";
    String lineRex;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
        this.idGenerator= IdGenerator.builder()
                .addHost("133.133.135.23", 6379, "c5809078fa6d652e0b0232d552a9d06d37fe819c")
//				.addHost("127.0.0.1", 7379, "accb7a987d4fb0fd85c57dc5a609529f80ec3722")
//				.addHost("127.0.0.1", 8379, "f55f781ca4a00a133728488e15a554c070b17255")
                .build();
        Jedis jedis = JedisUtil.getJedis();
        String s=jedis.get("timeRex");
        timeRex= s==null?timeRex:s;
        String s2=jedis.get("extraRex");
        extraRex= s2==null?extraRex:s2;
        String s3=jedis.get("messageRex");
        messageRex= s3==null?messageRex:s3;
        this.lineRex=timeRex+"\\s+"+extraRex+"\\s+"+messageRex;
        jedis.close();
    }
    @Override
    public void execute(Tuple input) {
        byte[] binary = input.getBinary(0); // 跨jvm传输数据，接收到的是字节数据
        String[] rexs=new String[]{timeRex,extraRex,messageRex};
        String line = new String(binary);
        Pattern p = Pattern.compile(lineRex);
        Matcher m = p.matcher(line);
        if(!m.find()){ collector.fail(input);return;}
        String [] result=new String[rexs.length];
        for(int i=0;i<rexs.length;i++) {
            Pattern pattern = Pattern.compile(rexs[i]);
            Matcher matcher = pattern.matcher(line);
            if(!matcher.find()){return;};
            if(i<rexs.length-1){
                result[i]=matcher.group();
            }else{
                result[i]=matcher.group(1);
            }
        }
        long id = idGenerator.next("log");
        String time=result[0];
        String param=result[1];
        String message=result[2];
        //System.out.println(id+"\t"+time+"\t"+param+"\t"+message+"\t"+line);
        collector.emit(new Values( ""+id,time,param,message,line));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","time","param", "message","log"));
    }

}
