package org.vsdata.market.collector.collect;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Time;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
//import
import java.util.Map;
import java.util.TimerTask;
import backtype.storm.topology.BasicOutputCollector;

public class ParseBolt extends BaseRichBolt {
    private OutputCollector _collector;
    JSONParser parser;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        parser  = new JSONParser();
        _collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String inp = input.getValue(0).toString();
        JSONArray arr = null;
        try {
            arr = (JSONArray) parser.parse(inp);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        _collector.emit(input,new Values(arr,arr));
        _collector.ack(input);
        System.out.println("OUT>> [" + Thread.currentThread().getId() + "] " + input.getValue(0).toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("writehbase", "REFER"));
    }
}