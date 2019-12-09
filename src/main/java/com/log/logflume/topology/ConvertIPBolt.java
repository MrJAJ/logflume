package com.log.logflume.topology;

import com.log.logflume.utils.JedisUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import redis.clients.jedis.Jedis;
import sun.print.PSPrinterJob;

import java.io.FileOutputStream;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

/**
 * 日志数据预处理Bolt，实现功能：
 *     1.提取实现业务需求所需要的信息：ip地址、客户端唯一标识mid
 *     2.查询IP地址所属地，并发送到下一个Bolt
 */
public class ConvertIPBolt extends BaseRichBolt {

    private OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        byte[] binary = input.getBinary(0);
        String line = new String(binary).replace("\r","");
        try {
            String[] s = line.split(" : ");
            String[] fields = s[0].split("\\s+");
            if (fields.length == 0) {
                return;
            }
            if (!fields[0].matches("^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|(\\d{2}(([02468][1235679])|([13579][01345789]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|(1[0-9])|(2[0-8]))))))?$")) {
                return;
            }
            String time = fields[0] + " " + fields[1];//日志时间
            String level = fields[2];//日志级别
            String logClass = fields[fields.length - 1];//日志产生类
            String api = "";
            String log = "";
            int quantity = 0;
            String uid = "";
            String type_code = "";
            //log参数
            String[] para = s[s.length - 1].split("\\s+");
            if (logClass.startsWith("com.ism")&&para.length == 5) {
                api = para[0];//接口名称
                log = para[1];//接口详情

                try {
                    quantity = Integer.parseInt(para[2]);//数量
                } catch (Exception e) {
                    return;
                }
                uid = para[3];//上传uid
                type_code = para[4];
            } else {
                log = s[s.length - 1];//接口详情
            }
            // 发送数据到下一个bolt
            this.collector.emit(new Values(time, level, logClass, api, log, quantity, type_code, uid));
            collector.ack(input);
        }catch (Exception e){
            return;
        }
    }

    /**
     * 定义了发送到下一个bolt的数据
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time", "level", "logClass", "api", "log", "quantity", "type_code","uid"));
    }
}
