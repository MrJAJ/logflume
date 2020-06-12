package com.log.logflume.bolt;

import com.log.logflume.Entity.AlarmParam;
import com.log.logflume.utils.JedisUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.Results;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;
import redis.clients.jedis.Jedis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ThresholdAlarmBolt extends BaseRichBolt {
    /**
     * kafkaSpout发送的字段名为bytes
     */
    private OutputCollector collector;
    KieSession ksession;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
        Jedis jedis = JedisUtil.getJedis();
        String rules = jedis.get("rules");
        jedis.close();
        KieServices kieServices = KieServices.Factory.get();
        KieFileSystem kfs = kieServices.newKieFileSystem();
        kfs.write("src/main/resources/rules/rules2.drl", rules.getBytes());
        KieBuilder kieBuilder = kieServices.newKieBuilder(kfs).buildAll();
        Results results = kieBuilder.getResults();
        if (results.hasMessages(org.kie.api.builder.Message.Level.ERROR)) {
            System.out.println(results.getMessages());
            throw new IllegalStateException("### errors ###");
        }
        KieContainer kieContainer = kieServices.newKieContainer(kieServices.getRepository().getDefaultReleaseId());
        KieBase kieBase = kieContainer.getKieBase();
        this.ksession = kieBase.newKieSession();
    }
    @Override
    public void execute(Tuple input) {
        String dayStr = input.getStringByField("dayStr");
        Jedis jedis = JedisUtil.getJedis();
        int errorNum = Integer.parseInt(jedis.hget("ERROR",dayStr));
        int logNum = Integer.parseInt(jedis.hget("LogNum",dayStr));
        jedis.close();
        float errorrate=0;
        if(logNum!=0) {
            errorrate = errorNum / logNum;
        }
        AlarmParam param=new AlarmParam();
        param.setErrorThreshold(errorNum);
        param.setErrorrateThreshold(errorrate);
        FactHandle handle = ksession.insert(param);
        ksession.fireAllRules();
        ksession.delete(handle);
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(""));
    }

}
