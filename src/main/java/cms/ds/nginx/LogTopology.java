package cms.ds.nginx;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import cms.ds.nginx.KafkaSpout;
import cms.ds.nginx.CountBolts;
import cms.ds.nginx.SaveBolts;

public class LogTopology {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new KafkaSpout(), 1);

        builder.setBolt("count", new CountBolts(), 5).fieldsGrouping("spout", new Fields("server"));
        builder.setBolt("save", new SaveBolts(), 2).fieldsGrouping("count", new Fields("server"));
        
//        System.setProperty("storm.conf.file", "storm.yaml");

        Config conf = new Config();

        conf.setNumWorkers(2);

        StormSubmitter.submitTopologyWithProgressBar("ds_nginx_log", conf, builder.createTopology());
    }

}
