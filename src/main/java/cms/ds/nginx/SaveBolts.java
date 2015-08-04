package cms.ds.nginx;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class SaveBolts extends BaseRichBolt {

    private static final long serialVersionUID = -7666702251317348470L;
    
    private OutputCollector collector;
    
    private MongoDatabase db;
    
    private HashMap<String, MongoCollection<Document>> collections = new HashMap<String, MongoCollection<Document>>();

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        @SuppressWarnings("resource")
        MongoClient conn = new MongoClient(Constants.MONGO_HOST);
        db = conn.getDatabase(Constants.MONGO_DB);
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String server = input.getStringByField("server");
        if(!collections.containsKey(server)){
            collections.put(server, db.getCollection(server));
        }
        Document filter = new Document();
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(input.getLong(3));
        filter.append("field", input.getString(0));
        filter.append("content", input.getString(1));
        filter.append("time", c.getTime());
        Document up = new Document("count", input.getInteger(2));
        collections.get(server).updateOne(filter, new Document("$inc", up), new UpdateOptions().upsert(true));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
