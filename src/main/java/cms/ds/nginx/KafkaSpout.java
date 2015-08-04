package cms.ds.nginx;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.json.JSONObject;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import cms.ds.nginx.Constants;

/**
 * ds.nginx.spout
 *
 */
public class KafkaSpout extends BaseRichSpout
{
    /**
     * 
     */
    private static final long serialVersionUID = 3133932128444988580L;
    
    private KafkaStream<byte[], byte[]> kafka_stream;
    private ConsumerIterator<byte[], byte[]> iter;
    private SpoutOutputCollector _collector;
    
    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "10000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(Constants.KAFKA_TOPIC, 1);
        _collector = collector;
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(Constants.ZOOKEEPER, Constants.CONSUMER_GROUP));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicMap);
        kafka_stream = consumerMap.get(Constants.KAFKA_TOPIC).get(0);
        iter = kafka_stream.iterator();
    }

    @Override
    public void nextTuple() {
        if(!iter.hasNext()){
            return;
        }
        MessageAndMetadata<byte[], byte[]> msg = iter.next();
        JSONObject log = new JSONObject(new String(msg.message()));
        String[] _c_info = log.get("comos_info").toString().split("=");
        String _p = (String) log.get("params");
        _collector.emit(new Values(log.get("remote"), log.get("method"), log.get("api"), _p, log.get("code"), log.get("size"), log.get("referer"), _c_info[_c_info.length-1], log.get("agent"), log.get("request_time"), log.get("time"), new String(msg.key())), _p.substring(0, 31>_p.length()?_p.length():31));
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("remote", "method", "api", "params", "code", "size", "referer", "remote_addr", "agent", "request_time", "time", "server"));
    }
}
