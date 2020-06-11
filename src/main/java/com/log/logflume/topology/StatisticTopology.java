package com.log.logflume.topology;

import com.log.logflume.bolt.*;

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
import org.apache.storm.topology.base.BaseRichSpout;
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
        hbaseConf.put("hbase.zookeeper.quorum","master1.hadoop.com,worker1.hadoop.com,worker2.hadoop.com");
        hbaseConf.put("zookeeper.znode.parent", "/hbase-unsecure"); // /hbase-unsecure 是你们zookeeper 的hbase存储信息的znode
        hbaseConf.put("hbase.zookeeper.property.clientPort","2181"); // zookeeper 的端口应该大家都知道
        hbaseConf.put("hbase.master","master1.hadoop.com:16000");
        config.put("hbase.conf",hbaseConf);

        BaseRichSpout kafkaSpout = new MySpout();
        //业务计算
        builder.setSpout("id_kafka_spout", kafkaSpout,1);


        SimpleHBaseMapper logmapper = new SimpleHBaseMapper()
                .withRowKeyField("id")
                .withColumnFields(new Fields("time","param","message","log"))
                .withColumnFamily("base_info");

        SimpleHBaseMapper logclumapper = new SimpleHBaseMapper()
                .withRowKeyField("id")
                .withColumnFields(new Fields("cluster","model","param"))
                .withColumnFamily("extra_info");

        builder.setBolt("extractBolt", new ExtractBolt(),2).shuffleGrouping("id_kafka_spout");

        builder.setBolt("replaceBolt", new ReplaceBolt(),2).shuffleGrouping("extractBolt");
//
//        HBaseBolt logHbaseBolt = new HBaseBolt("log_info", logmapper).withConfigKey("hbase.conf");
//        builder.setBolt("logHBaseBolt", logHbaseBolt,2).shuffleGrouping("replaceBolt");

//        builder.setBolt("extraCountBolt", new ExtraCountBolt(),3).shuffleGrouping("extractBolt");
//
        builder.setBolt("spliteSimBolt", new SpliteSimBolt(),6).shuffleGrouping("replaceBolt");
//
//        builder.setBolt("clusterCountBolt", new ClusterCountBolt(),3).shuffleGrouping("spliteSimBolt");
//
        builder.setBolt("clusterSpellBolt", new ClusterSpellBolt(),4).shuffleGrouping("spliteSimBolt");

        //builder.setBolt("modelSpliteBolt", new ModelSpliteBolt(),4).shuffleGrouping("clusterSpellBolt");

        //builder.setBolt("modelAnomyDetectionBolt", new ModelAnomyDetectionBolt(),4).fieldsGrouping("modelSpliteBolt",new Fields("uid"));

        //builder.setBolt("WorkFlowBolt", new WorkFlowBolt(),4).shuffleGrouping("modelAnomyDetectionBolt");

        builder.setBolt("variableSpliteBolt", new VariableSpliteBolt(),4).fieldsGrouping("clusterSpellBolt",new Fields("model"));

        builder.setBolt("singleDBScanBolt", new SingleDBScanBolt(),4).fieldsGrouping("variableSpliteBolt",new Fields("model"));

        builder.setBolt("gloableDBScanBolt", new GloableDBScanBolt(),4).fieldsGrouping("singleDBScanBolt",new Fields("model"));

        builder.setBolt("paramAnomyDetectionBolt", new ParamAnomyDetectionBolt(),4).fieldsGrouping("gloableDBScanBolt",new Fields("model"));

        builder.setBolt("anomyAlarmBolt", new AnomyAlarmBolt(),4).fieldsGrouping("paramAnomyDetectionBolt",new Fields("type"));



//        HBaseBolt logcluHbaseBolt = new HBaseBolt("log_info", logclumapper)
//                .withConfigKey("hbase.conf");
//        builder.setBolt("logcluHBaseBolt", logcluHbaseBolt,2).shuffleGrouping("clusterSpellBolt");

        // 使用builder构建topology
        StormTopology topology = builder.createTopology();
        String topologyName =StatisticTopology.class.getSimpleName();   // 拓扑的名称

        config.setNumWorkers(4);
        // 启动topology，本地启动使用LocalCluster，集群启动使用StormSubmitter
        if (args == null || args.length < 1) {  // 没有参数时使用本地模式，有参数时使用集群模式
            LocalCluster localCluster = new LocalCluster(); // 本地开发模式，创建的对象为LocalCluster
            localCluster.submitTopology(topologyName, config, topology);
            //Thread.sleep(1000);

            //localCluster.shutdown();
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
        String id = "consumer-id";
        SpoutConfig spoutConf = new SpoutConfig(hosts, topic, zkRoot, id);
        // 本地环境设置之后，也可以在zk中建立/f-k-s节点，在集群环境中，不用配置也可以在zk中建立/f-k-s节点
        //spoutConf.zkServers = Arrays.asList(new String[]{"uplooking01", "uplooking02", "uplooking03"});
        //spoutConf.zkPort = 2181;
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
