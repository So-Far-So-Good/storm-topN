package com.cc.storm.bolt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

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

public class Merchandise2UserBolt implements IBasicBolt {

	/**
	 * @Fields serialVersionUID : TODO
	 */
	private static final long serialVersionUID = 7471527958792933013L;
	public static Logger LOG = Logger.getLogger(Merchandise2UserBolt.class);
	
	@SuppressWarnings("rawtypes")
	private HashMap<Object, HashSet> merchandise2user = new HashMap<Object, HashSet>();
	/**
	 * @Fields serialVersionUID : TODO
	 */
	Long _lastTime = null;
	
	JedisPool pool;
	Jedis jedis;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("merchandise2user"));
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
		HashSet userList = merchandise2user.get(merchandiseID);
		if( null == userList) {
			userList = new HashSet();
		}
		userList.add(userID);
		merchandise2user.put(merchandiseID, userList);
		long currentTime = System.currentTimeMillis();
		if(_lastTime == null || currentTime >= _lastTime + 2000) {
			jedis.lpush("merchandise2user", JSONValue.toJSONString(merchandise2user));
			_lastTime = currentTime;
		}
		
		/**
		 * 保持固定长度，避免内存占用过多溢出
		 */
		long len = jedis.llen("merchandise2user");
		if(len > 100) {
			jedis.rpop("merchandise2user");
		}
		
//		collector.emit(new Values(JSONValue.toJSONString(merchandise2user)));
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		pool.returnResource(jedis);
	}

	
}


