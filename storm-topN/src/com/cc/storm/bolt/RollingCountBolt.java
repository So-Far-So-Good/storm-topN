package com.cc.storm.bolt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/** 
 * @author chencheng	QQ:4998170  
 * @date 2013-3-14 上午10:17:43
 * @version V1.0   
 */

public class RollingCountBolt implements IRichBolt{

	/**
	 * @Fields serialVersionUID : TODO
	 */
	private static final long serialVersionUID = 1765379339552134320L;

	private HashMap<Object, long[]> _objectCounts = new HashMap<Object, long[]>();
	private int _numBuckets;
	private transient Thread cleaner;
	private OutputCollector _collector;
	private int _trackMinutes;
	
	
	public RollingCountBolt(int numBuckets, int trackMinutes) {
		// TODO Auto-generated constructor stub
		this._numBuckets = numBuckets;
		this._trackMinutes = trackMinutes;
	}
	
	public long totalObjects(Object obj) {
		long[] curr = _objectCounts.get(obj);
		long total = 0;
		for(long l : curr) {
			total += l;
		}
		return total;
	}
	
	public int currentBucket(int buckets) {
		return currentSecond() / secondsPerBucket(buckets) % buckets;
	}
	
	public int currentSecond() {
		return (int) (System.currentTimeMillis() / 1000);
	}
	
	public int secondsPerBucket(int buckets) {
		return _trackMinutes * 60 / buckets;
	}
	
	public long millisPerBucket(int buckets) {
		return (long) 1000 * secondsPerBucket(buckets);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		cleaner = new Thread(new Runnable() {
			
			@SuppressWarnings("unchecked")
			@Override
			public void run() {
				// TODO Auto-generated method stub
				int lastBucket = currentBucket(_numBuckets);
				
				while(true) {
					int currBucket = currentBucket(_numBuckets);
					if(currBucket != lastBucket) {
						int bucketToWipe = (currBucket + 1) % _numBuckets;
//						System.out.println("bucketToWipe:\t"+bucketToWipe);
						synchronized (_objectCounts) {
							Set objs = new HashSet(_objectCounts.keySet());
							for(Object obj : objs) {
								long[] counts = _objectCounts.get(obj);
//								System.out.println("long[] counts = _objectCounts.get(obj)\t"+counts);
								long currBucketVal = counts[bucketToWipe];
								counts[bucketToWipe] = 0;
								long total = totalObjects(obj);
//								System.out.println("long total = totalObjects(obj):\t"+total);
								if(currBucketVal != 0) {
									_collector.emit(new Values(obj, total));
								}
								if(total == 0) {
									_objectCounts.remove(obj);
								}
							}
						}
						lastBucket = currBucket;
					}
					
					long delta = millisPerBucket(_numBuckets) - 
							(System.currentTimeMillis() % millisPerBucket(_numBuckets));
					Utils.sleep(delta);
				}
			}
		});
		cleaner.start();
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		Object obj1 = input.getValue(0);
		Object obj = input.getValue(1);
		int bucket = currentBucket(_numBuckets);
		synchronized (_objectCounts) {
			long[] curr = _objectCounts.get(obj);
			if(curr == null) {
				curr = new long[_numBuckets];
				_objectCounts.put(obj, curr);
			}
			curr[bucket]++;
			_collector.emit(new Values(obj, totalObjects(obj)));
			_collector.ack(input);
		}
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


