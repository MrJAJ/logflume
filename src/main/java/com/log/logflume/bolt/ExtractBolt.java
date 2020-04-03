package com.log.logflume.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ExtractBolt extends BaseRichBolt {
    /**
     * kafkaSpout发送的字段名为bytes
     */
    private OutputCollector collector;
    static AtomicInteger id=new AtomicInteger();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
        id.getAndSet(0);
    }
    @Override
    public void execute(Tuple input) {
        byte[] binary = input.getBinary(0); // 跨jvm传输数据，接收到的是字节数据
        String line = new String(binary).replace("\r","");
        if (line.equals("") || line.indexOf(" : ") == -1) {
            System.out.println(line);
            return;
        }
        String[] vars = line.split(" : ")[0].split(" ");
        if(vars.length<4){
            System.out.println(line);
            return;
        }
        String time=vars[0]+" "+vars[1];
        String level=vars[2];
        String logClass=vars[vars.length-1];
        String detail = line.split(" : ")[1].replaceAll("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}：[0-9]{2}：[0-9]{2}(：[0-9]{3})?", "");
        System.out.println(id.incrementAndGet()+" "+time+" "+level+" "+logClass+" "+detail);
        if(id.get()<10) {
            collector.emit(new Values( level, 1));
            collector.ack(input);
        }else {
            return;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("level", "count"));
    }

}
