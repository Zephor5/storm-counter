/**
 * 
 */
package cms.ds.nginx;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * @author starwu
 *
 */
public class CountBolts extends BaseRichBolt {

    private static final long serialVersionUID = 4541983102677510376L;
    
    private static final HashSet<String> FIELDS = new HashSet<String>(Arrays.asList("remote", "method", "api", "params", "code", "size", "referer", "remote_addr", "agent", "request_time"));

    private static final HashSet<String> SKIP_FIELDS = new HashSet<String>(Arrays.asList("method", "params", "size", "referer", "agent"));
    
    private OutputCollector collector;
    
    private HashMap<String, Calendar> _timers = new HashMap<String, Calendar>();
    
    private HashMap<String, HashMap<String, HashMap<String, Integer>>> counters = new HashMap<String, HashMap<String, HashMap<String, Integer>>>();

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String server = input.getStringByField("server");
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(input.getIntegerByField("time") * 1000L);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        if(!counters.containsKey(server)){
            counters.put(server, new HashMap<String, HashMap<String, Integer>>());
        }
        List<List<Object>> tups = new ArrayList<List<Object>>();
        HashMap<String, HashMap<String, Integer>> counter;
        if(!_timers.containsKey(server)){
            _timers.put(server, c);
        }
        Calendar _time = _timers.get(server);
        if(_time.before(c)){
            counter = counters.get(server);
            _timers.put(server, c);
            counters.put(server, new HashMap<String, HashMap<String, Integer>>());
            for(String field: counter.keySet()){
                for(String content: counter.get(field).keySet()){
                    List<Object> tup = Arrays.asList(field, content, counter.get(field).get(content), _time.getTimeInMillis(), server);
                    tups.add(tup);
                }
            }
        }
        counter = counters.get(server);
        for(String field: FIELDS){
            if(SKIP_FIELDS.contains(field)){
                continue;
            }
            String val = input.getStringByField(field);
            if(field.equals("request_time")){
                Float t = Float.valueOf(val);
                if(t>=5){
                    val = "5<=";
                }
                else if(t>=1&&t<5){
                    val = "1~5";
                }
                else if(t>=0.5&&t<1){
                    val = "0.5~1";
                }
                else if(t>=0.05&&t<0.5){
                    val = "0.05~0.5";
                }
                else{
                    val = "<0.05";
                }
            }
            if(!counter.containsKey(field)){
                counter.put(field, new HashMap<String, Integer>());
            }
            Integer _c = 0;
            if(counter.get(field).containsKey(val)){
                _c = counter.get(field).get(val);
            }
            counter.get(field).put(val, _c+1);
        }
        if(tups.size()>0){
            for(List<Object> tup:tups){
                collector.emit(tup);
            }
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("field", "content", "count", "time", "server"));
    }

}
