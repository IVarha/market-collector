package org.vsdata.market.collector.collect;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by igorv on 06.06.15.
 */
public class CountBolt extends BaseRichBolt {
    private OutputCollector _collector;
    Map<String, Long> counts = new HashMap<String, Long>();

    int DAY = 86400;
    Thread thread = new Thread(){
        @Override
        public void run() {
            try {
                Thread.sleep(DAY*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    };





    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        Long count = counts.get(word);
        if (count == null)
            count = (long)0;
        count++;
        counts.put(word, count);
        _collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
