package com.cc.storm.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/** 
 * @author chencheng	QQ:4998170  
 * @date 2013-3-14 上午8:59:48
 * @version V1.0   
 * @category 数据收集
 */
public class RedisPubSubSpout extends BaseRichSpout {

	/**
	 * @Fields serialVersionUID : TODO
	 */
	private static final long serialVersionUID = 4092527421163270357L;
	static Logger LOG = Logger.getLogger(RedisPubSubSpout.class);
	
	private SpoutOutputCollector _collector;
	private final String host;
	private final int port;
	private final String pattern;
	
	LinkedBlockingQueue<String> queue;
	
	JedisPool pool;
	
	public RedisPubSubSpout(String host, int port, String pattern) {
		// TODO Auto-generated constructor stub
		this.host = host;
		this.port = port;
		this.pattern = pattern;
	}
	
	//监听线程，从redis订阅的兴趣事件中获取数据
	class ListenerThread extends Thread {
		private LinkedBlockingQueue<String> queue;
		JedisPool pool;
		String pattern;
		
		public ListenerThread(LinkedBlockingQueue<String> queue, JedisPool pool, String pattern) {
			// TODO Auto-generated constructor stub
			this.queue = queue;
			this.pool = pool;
			this.pattern = pattern;
		}
		
		@Override
		public void run() {
			JedisPubSub listener = new JedisPubSub() {
				
				@Override
				public void onUnsubscribe(String arg0, int arg1) {
					// TODO Auto-generated method stub
					
				}
				
				@Override
				public void onSubscribe(String arg0, int arg1) {
					// TODO Auto-generated method stub
					
				}
				
				@Override
				public void onPUnsubscribe(String arg0, int arg1) {
					// TODO Auto-generated method stub
					
				}
				
				@Override
				public void onPSubscribe(String arg0, int arg1) {
					// TODO Auto-generated method stub
					
				}
				
				@Override
				public void onPMessage(String pattern, String channel, String message) {
					// TODO Auto-generated method stub
					queue.offer(message);
				}
				
				@Override
				public void onMessage(String channel, String message) {
					// TODO Auto-generated method stub
					queue.offer(message);
				}
			};
			
			Jedis jedis = pool.getResource();
			
			try {
				jedis.psubscribe(listener, pattern);
			} finally {
				pool.returnResource(jedis);
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		//队列最大支持1000个
		queue = new LinkedBlockingQueue<String>(1000);
		JedisPoolConfig config = new JedisPoolConfig();
		pool = new JedisPool(config, host, port);
		
		ListenerThread listener = new ListenerThread(queue, pool, pattern);
		//启动线程
		listener.start();
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		String ret = queue.poll();
		if(null == ret) {
			//如果队列中暂无数据可取，休息500ms
			Utils.sleep(500);
		} else {
			//数据格式为 “userID:merchandiseID”，可以依据需求更改此处
			String[] s = ret.split(":");
			_collector.emit(new Values(s[0], s[1]));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("userIdS","merchandiseIDS"));
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		pool.destroy();
	}
}


