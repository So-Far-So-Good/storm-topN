package com.cc.storm.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/** 
 * @author chencheng	QQ:4998170  
 * @date 2013-3-14 上午10:17:43
 * @version V1.0   
 */

public class RollingAllCountBolt implements IRichBolt{

	/**
	 * @Fields serialVersionUID : TODO
	 */
	private static final long serialVersionUID = 1765379339552134320L;

	private HashMap<Object, Long> _objectCounts = new HashMap<Object, Long>();
	private OutputCollector _collector;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;

	}

	@Override
	public void execute(Tuple input) {
		
		String word = input.getString(0);
        Long count = _objectCounts.get(word);
        if(count == null) count = 0L;
        count++;
        _objectCounts.put(word, count);
        _collector.emit(new Values(word, count));
        _collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("merchandiseID", "count"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}


