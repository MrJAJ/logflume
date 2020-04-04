package com.log.logflume.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExtractBolt extends BaseRichBolt {
    /**
     * kafkaSpout发送的字段名为bytes
     */
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
    }
    @Override
    public void execute(Tuple input) {
        byte[] binary = input.getBinary(0); // 跨jvm传输数据，接收到的是字节数据
        //ID生成
        IdGenerator idGenerator = IdGenerator.builder()
                .addHost("133.133.135.38", 6379, "c5809078fa6d652e0b0232d552a9d06d37fe819c")
//				.addHost("127.0.0.1", 7379, "accb7a987d4fb0fd85c57dc5a609529f80ec3722")
//				.addHost("127.0.0.1", 8379, "f55f781ca4a00a133728488e15a554c070b17255")
                .build();

        //正则提取表达式
        String timeRex="(\\d+-\\d+-\\d+\\s\\d+:\\d+:\\d+.\\d+)\\s";
        String extraRex="(\\S+)\\s\\d+\\s---\\s\\[([^\\]]*)\\]\\s+(\\S+)";
        String messageRex=":\\s(.*)";
        String[] rexs=new String[]{timeRex,extraRex,messageRex};

        String line = new String(binary);
        if (line.equals("")) {
            return;
        }
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
        collector.emit(new Values( id,time,param,message));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","time","param", "message"));
    }

}
