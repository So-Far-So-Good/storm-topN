package com.cc.storm.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.simple.JSONValue;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/** 
 * @author chencheng	QQ:4998170  
 * @date 2013-3-14 下午2:21:44
 * @version V1.0   
 */

public class User2MerchandiseBolt implements IBasicBolt {

	public static Logger LOG = Logger.getLogger(User2MerchandiseBolt.class);
	/**
	 * @Fields serialVersionUID : TODO
	 */
	private static final long serialVersionUID = -1729847516839455550L;
	
	
	@SuppressWarnings("rawtypes")
	private HashMap<Object, HashSet> user2merchandise = new HashMap<Object, HashSet>();
	Long _lastTime = null;
	
	JedisPool pool;
	Jedis jedis;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("user2merchandise"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		pool = new JedisPool("192.168.200.216", 6379);
		jedis = pool.getResource();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		Object userID = input.getValue(0);
		Object merchandiseID = input.getValue(1);
		HashSet mermList = user2merchandise.get(userID);

		if( null == mermList) {
			mermList = new HashSet();
		}
		mermList.add(merchandiseID);
		user2merchandise.put(userID, mermList);
		long currentTime = System.currentTimeMillis();
		if(_lastTime == null || currentTime >= _lastTime + 2000) {
			jedis.lpush("user2merchandise", JSONValue.toJSONString(user2merchandise));
			_lastTime = currentTime;
		}
		
		/**
		 * 保持固定长度，避免内存占用过多溢出
		 */
		long len = jedis.llen("user2merchandise");
		if(len > 100) {
			jedis.rpop("user2merchandise");
		}
//		collector.emit(new Values(JSONValue.toJSONString(user2merchandise)));
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		pool.returnResource(jedis);
	}

	
}


