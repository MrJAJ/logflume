package com.log.logflume.topology;

import com.log.logflume.utils.JedisUtil;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 日志数据统计Bolt，实现功能：
 * 1.统计各省份的PV、UV
 * 2.以天为单位，将省份对应的PV、UV信息写入Redis
 */
public class StatisticBolt extends BaseRichBolt {

    Map<String, Integer> levelMap = new HashMap<>();
    Map<String, Integer> apiMap = new HashMap<>();
    Map<String, Integer> quantityMap = new HashMap<>();
    Map<String, Integer> uidMap = new HashMap<>();
    Map<String, Integer> dateMap = new HashMap<>();
    Map<String, Integer> typeMap = new HashMap<>();
    Map<String, Integer> logClassMap = new HashMap<>();
    Map<String, HashSet<String>> midsMap = null;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH");
    SimpleDateFormat sdfDay = new SimpleDateFormat("yyyyMMdd");
    private final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple input) {
        //if (!input.getSourceComponent().equalsIgnoreCase(Constants.SYSTEM_COMPONENT_ID)) {  // 如果收到非系统级别的tuple，统计信息到局部变量mids
            String time = input.getStringByField("time");
            String level = input.getStringByField("level");
            String log = input.getStringByField("log");
            String logClass = input.getStringByField("logClass");
            Date date=null;
            try {
                date=sdf2.parse(time);
            } catch (ParseException e) {
                e.printStackTrace();
        }
            Jedis jedis = JedisUtil.getJedis();
            String dateStr = sdf.format(date);

            jedis.sadd("logClass","l-"+logClass);
            logClassMap.put("l-"+logClass, logClassMap.getOrDefault(logClass, 0) + 1);
            if(!input.getStringByField("api").equals("")) {
                int quantity = input.getIntegerByField("quantity");
                String api = input.getStringByField("api");
                String uid = input.getStringByField("uid");
                String type_code = input.getStringByField("type_code");
                jedis.sadd("api",api);
                jedis.sadd("uid",uid);
                levelMap.put(level, levelMap.getOrDefault(level, 0) + 1);   // pv+1
                if (level.equals("INFO")) {
                    quantityMap.put(level, quantityMap.getOrDefault(level, 0) + quantity);
                }
                typeMap.put(type_code, typeMap.getOrDefault(type_code, 0) + 1);   // pv
                apiMap.put(api, apiMap.getOrDefault(api, 0) + 1);
                uidMap.put(uid, uidMap.getOrDefault(uid, 0) + 1);
                dateMap.put(time, dateMap.getOrDefault(time, 0) + 1);
                //System.out.println(time + " " + level + " " + api + " " + quantity + " " + uid + " " + type_code + " " + log);
            }else{
                levelMap.put(level, levelMap.getOrDefault(level, 0) + 1);   // pv+1
                //System.out.println(time + " " + level + " " + log);
            }
        JedisUtil.returnJedis(jedis);
            save(date);
//        } else {    // 如果收到系统级别的tuple，则将数据更新到Redis中，释放JVM堆内存空间


            // 更新midsMap到Redis中
//            String midsKey = null;
//            HashSet<String> midsSet = null;
//            for(String province: midsMap.keySet()) {
//                midsSet = midsMap.get(province);
//                if(midsSet.size() > 0) {  // 当前省份的set的大小大于0才更新到，否则没有意义
//                    midsKey = province + "_mids_" + dateStr;
//                    jedis.sadd(midsKey, midsSet.toArray(new String[midsSet.size()]));
//                    midsSet.clear();
//                }
//            }
            // 释放jedis资源

//            System.out.println(System.currentTimeMillis() + "------->写入数据到Redis");
//        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public void save(Date date){
        String dateStr = sdf.format(date);
        saveMap(levelMap,dateStr);
        saveMap(apiMap,dateStr);
        saveMap(quantityMap,dateStr);
        saveMap(uidMap,dateStr);
        saveMap(typeMap,dateStr);
        saveMap(logClassMap,dateStr);

    }
    public void saveMap(Map<String, Integer> map,String dateStr){
        Jedis jedis = JedisUtil.getJedis();
        String dayStr=dateStr.split(" ")[0];
        // 更新pvMap数据到Redis中
        String levelKey = null;
        for (String l : map.keySet()) {
            int currentPv = map.get(l);
            if (currentPv > 0) { // 当前map中的pv大于0才更新，否则没有意义
                levelKey = l;
                String oldLevelStr = jedis.hget(levelKey, dateStr);
                String olddayStr = jedis.hget(levelKey, dayStr);
                if (oldLevelStr == null) {
                    oldLevelStr = "0";
                }if (olddayStr == null) {
                    olddayStr = "0";
                }

                int oldLevel = Integer.valueOf(oldLevelStr);
                int oldDay = Integer.valueOf(olddayStr);
                jedis.hset(levelKey, dateStr, oldLevel + currentPv + "");
                jedis.hset(levelKey, dayStr,  oldDay+ currentPv + "");
                map.replace(levelKey, 0); // 将该省的pv重新设置为0
            }
        }
        JedisUtil.returnJedis(jedis);
    }
}
