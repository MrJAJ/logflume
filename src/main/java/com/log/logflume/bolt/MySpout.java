package com.log.logflume.bolt;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

//数据源，在已知的英文句子中，随机发送一条句子出去。
public class MySpout extends BaseRichSpout {

    //用来收集Spout输出的tuple
    private SpoutOutputCollector collector;
    private Random random;


    //该方法调用一次，主要由storm框架传入SpoutOutputCollector
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        random = new Random();
        //连接kafka mysql ,打开本地文件
    }

    /**
     * 上帝之手
     while(true)
           spout.nextTuple()
     */
    @Override
    public void nextTuple() {
        String[] sentences = new String[]{
                "2019-04-25 00:02:53.085 ERROR 256668 --- [http-nio-8081-exec-2] com.ism.service.UnitService              : 2019-04-25 00：02：52主体代码为 '350206197701151014' 的企业已经备案，请检查您的提交数据",
                "2019-04-25 00:02:56.790 ERROR 256668 --- [http-nio-8081-exec-6] c.ism.exception.CustomExceptionHandler   : 2019-04-25 00：02：56：790 data.comp_record_info[0].unit_base_info.unit_code 字段错误。 错误原因: 个数必须在18和18之间",
                "2019-04-25 00:27:42.979 ERROR 256668 --- [http-nio-8081-exec-10] com.ism.service.TransactionService       : 2019-04-25 00：27：42第1条、交易信息中第1条交易明细信息中的产品未登记，交易单号为30521100100000125669，产品追溯码为350690129921113011",
                "2019-04-25 00:51:05.929 ERROR 256668 --- [http-nio-8081-exec-6] com.ism.service.TransactionService       : 2019-04-25 00：51：05交易信息列表登记异常\n",
                "2019-04-25 00:54:42.129 ERROR 256668 --- [http-nio-8081-exec-9] com.ism.service.TransactionService       : 2019-04-25 00：54：42第1条、交易信息中第1条交易明细信息中的产品未登记，交易单号为30521100100000135584，产品追溯码为350705100221113011\n"
        };
        String sentence = sentences[random.nextInt(sentences.length)];
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        collector.emit(new Values(sentence.getBytes()));
    }

    //消息源可以发射多条消息流stream
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}