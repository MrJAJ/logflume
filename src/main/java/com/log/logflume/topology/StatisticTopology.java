package com.log.logflume.topology;

import com.log.logflume.bolt.CountBolt;
import com.log.logflume.bolt.ExtractBolt;
import com.log.logflume.bolt.ReplaceBolt;
import kafka.api.OffsetRequest;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;

/**
 * 构建topology
 */
public class StatisticTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        Config config = new Config();
        Map<String, Object> hbaseConf = new HashMap<String, Object>();
        hbaseConf.put("hbase.rootdir","hdfs://master1.hadoop.com:8020/hbase");
        hbaseConf.put("hbase.zookeeper.quorum", "master1.hadoop.com:2181,worker1.hadoop.com:2181,worker2.hadoop.com:2181");
        config.put("hbase.conf",hbaseConf);

        /**
         * 设置spout和bolt的dag（有向无环图）
         */
        KafkaSpout kafkaSpout = createKafkaSpout();
        //业务计算
        builder.setSpout("id_kafka_spout", kafkaSpout);
        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField("level")
                .withColumnFields(new Fields("level"))
                .withCounterFields(new Fields("count"))
                .withColumnFamily("base_info");

        builder.setBolt("extractBolt", new ExtractBolt(),2).shuffleGrouping("id_kafka_spout");

        builder.setBolt("replaceBolt", new ReplaceBolt(),3).shuffleGrouping("extractBolt");

        builder.setBolt("countBolt", new CountBolt(),3).shuffleGrouping("extractBolt");

        //HBaseBolt hbaseBolt = new HBaseBolt("t_log_info", mapper)
                //.withConfigKey("hbase.conf");//如果没有withConfigKey会报错
        //builder.setBolt("HBaseBolt", hbaseBolt).shuffleGrouping("extractBolt");

        //builder.setBolt("id_myKafa_bolt", new MyKafkaBolt()).shuffleGrouping("id_kafka_spout");

        //builder.setBolt("id_convertIp_bolt", new ConvertIPBolt()).shuffleGrouping("id_kafka_spout"); // 通过不同的数据流转方式，来指定数据的上游组件
        //builder.setBolt("id_statistic_bolt", new StatisticBolt()).shuffleGrouping("id_convertIp_bolt"); // 通过不同的数据流转方式，来指定数据的上游组件

        //web
        //KafkaSpout webSpout = createWebSpout();
        //builder.setSpout("id_web_spout", webSpout);
        //builder.setBolt("id_web_bolt", new MyKafkaBolt()).shuffleGrouping("id_web_spout");
        //builder.setBolt("id_webConvert_bolt", new WebConvertBolt()).shuffleGrouping("id_web_spout");
        //builder.setBolt("id_webProcess_bolt", new WebProcessBolt()).shuffleGrouping("id_webConvert_bolt");

        //日志聚类
        //builder.setBolt("id_logConvert_bolt", new LogConvertBolt()).shuffleGrouping("id_convertIp_bolt");

        // 使用builder构建topology
        StormTopology topology = builder.createTopology();
        String topologyName =StatisticTopology.class.getSimpleName();   // 拓扑的名称


        // 启动topology，本地启动使用LocalCluster，集群启动使用StormSubmitter
        if (args == null || args.length < 1) {  // 没有参数时使用本地模式，有参数时使用集群模式
            LocalCluster localCluster = new LocalCluster(); // 本地开发模式，创建的对象为LocalCluster
            localCluster.submitTopology(topologyName, config, topology);
        } else {
            StormSubmitter.submitTopology(topologyName, config, topology);
        }
    }

    /**
     * BrokerHosts hosts  kafka集群列表
     * String topic       要消费的topic主题
     * String zkRoot      kafka在zk中的目录（会在该节点目录下记录读取kafka消息的偏移量）
     * String id          当前操作的标识id
     */
    private static KafkaSpout createKafkaSpout() {
        String brokerZkStr = "master1.hadoop.com:2181,worker1.hadoop.com:2181,worker2.hadoop.com:2181";
        BrokerHosts hosts = new ZkHosts(brokerZkStr);   // 通过zookeeper中的/brokers即可找到kafka的地址
        String topic = "uploadLogs";
        String zkRoot = "/" + topic;
        String id = "consumer-3";
        SpoutConfig spoutConf = new SpoutConfig(hosts, topic, zkRoot, id);
        // 本地环境设置之后，也可以在zk中建立/f-k-s节点，在集群环境中，不用配置也可以在zk中建立/f-k-s节点
        //spoutConf.zkServers = Arrays.asList(new String[]{"uplooking01", "uplooking02", "uplooking03"});
        //spoutConf.zkPort = 2181;
        //spoutConf.startOffsetTime=OffsetRequest.EarliestTime();

        //spoutConf.startOffsetTime = OffsetRequest.LatestTime(); // 设置之后，刚启动时就不会把之前的消息也进行读取，会从最新的偏移量开始读取
        return new KafkaSpout(spoutConf);
    }

    private static KafkaSpout createWebSpout() {
        String brokerZkStr = "master1.hadoop.com:2181,worker1.hadoop.com:2181,worker2.hadoop.com:2181";
        BrokerHosts hosts = new ZkHosts(brokerZkStr);   // 通过zookeeper中的/brokers即可找到kafka的地址
        String topic = "webTopic";
        String zkRoot = "/" + topic;
        String id = "consumer-id";
        SpoutConfig spoutConf = new SpoutConfig(hosts, topic, zkRoot, id);
        // 本地环境设置之后，也可以在zk中建立/f-k-s节点，在集群环境中，不用配置也可以在zk中建立/f-k-s节点
        //spoutConf.zkServers = Arrays.asList(new String[]{"uplooking01", "uplooking02", "uplooking03"});
        //spoutConf.zkPort = 2181;
        //spoutConf.startOffsetTime = OffsetRequest.LatestTime(); // 设置之后，刚启动时就不会把之前的消息也进行读取，会从最新的偏移量开始读取
        return new KafkaSpout(spoutConf);
    }
}
