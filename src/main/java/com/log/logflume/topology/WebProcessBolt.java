package com.log.logflume.topology;

import com.log.logflume.utils.JedisUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
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
public class WebProcessBolt extends BaseRichBolt {

    Map<String, Integer> ipMap = new HashMap<>();
    Map<String, Integer> apiMap = new HashMap<>();
    Map<String, Integer> quantityMap = new HashMap<>();
    Map<String, Integer> uidMap = new HashMap<>();
    Map<String, Integer> dateMap = new HashMap<>();
    Map<String, Integer> typeMap = new HashMap<>();
    Map<String, HashSet<String>> midsMap = null;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH");
    SimpleDateFormat sdfDay = new SimpleDateFormat("yyyyMMdd");
    private final SimpleDateFormat webSdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple input) {
        //if (!input.getSourceComponent().equalsIgnoreCase(Constants.SYSTEM_COMPONENT_ID)) {  // 如果收到非系统级别的tuple，统计信息到局部变量mids
            String time = input.getStringByField("time");
            Date date=null;
        String dateStr="";
        String dayStr="";
            try {
                date=webSdf.parse(time);
                dateStr = sdf.format(date);
                dayStr = sdfDay.format(date);
            } catch (ParseException e) {
                //e.printStackTrace();
                return;
            }
            String ip=input.getStringByField("ip");
            Jedis jedis = JedisUtil.getJedis();
            String oldLevelStr = jedis.hget(ip, dateStr);
            if (oldLevelStr == null) {
                oldLevelStr = "0";
            }
            int oldLevel = Integer.valueOf(oldLevelStr);
            jedis.hset(ip, dateStr, oldLevel + 1 + "");
            jedis.sadd("ip",ip);

            String oldLevelDayStr = jedis.hget(ip, dayStr);
            if (oldLevelDayStr == null) {
                oldLevelDayStr = "0";
            }
            int oldLevelDay = Integer.valueOf(oldLevelDayStr);
            jedis.hset(ip, dayStr, oldLevelDay + 1 + "");

            String[] para=input.getStringByField("para").split(" ");
            String oldStatusCount = jedis.hget(para[0], dateStr);
            if (oldStatusCount == null) {
                oldStatusCount = "0";
            }
            int oldStatus = Integer.valueOf(oldStatusCount);
            jedis.hset(para[0], dateStr, oldStatus + 1 + "");

            JedisUtil.returnJedis(jedis);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public void saveMap(Map<String, Integer> map){
        Jedis jedis = JedisUtil.getJedis();
        String dateStr = sdf.format(new Date());
        // 更新pvMap数据到Redis中
        String levelKey = null;
        for (String l : map.keySet()) {
            int currentPv = map.get(l);
            if (currentPv > 0) { // 当前map中的pv大于0才更新，否则没有意义
                levelKey = l;
                String oldLevelStr = jedis.hget(levelKey, dateStr);
                if (oldLevelStr == null) {
                    oldLevelStr = "0";
                }
                int oldLevel = Integer.valueOf(oldLevelStr);
                jedis.hset(levelKey, dateStr, oldLevel + currentPv + "");
                map.replace(levelKey, 0); // 将该省的pv重新设置为0
            }
        }
        JedisUtil.returnJedis(jedis);
    }
}
