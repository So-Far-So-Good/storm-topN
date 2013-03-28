package com.cc.storm;


import com.cc.storm.bolt.Merchandise2UserBolt;
import com.cc.storm.bolt.MergeBolt;
import com.cc.storm.bolt.RankBolt;
import com.cc.storm.bolt.RollingAllCountBolt;
import com.cc.storm.bolt.RollingCountBolt;
import com.cc.storm.bolt.User2MerchandiseBolt;
import com.cc.storm.spout.RedisPubSubSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/** 
 * @author chencheng	QQ:4998170  
 * @date 2013-3-14 上午9:48:46
 * @version V1.0   
 */

public class TOP10 {

	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {

		if(args.length != 2) {
			System.out.println("参数不匹配...");
			System.out.println("两个参数分别是：\n \tparam:\t指定TOP-N，数字类型\n \tparam:\t指定时间周期，数字类型");
			System.exit(1);
		}
		final int TOP_N = Integer.parseInt(args[0]);
		final int time = Integer.parseInt(args[1]);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("$datasource$", new RedisPubSubSpout("SNS2", 6379, "news"),2);
//		builder.setBolt("$count$", new RollingAllCountBolt(), 2)
//				.fieldsGrouping("$datasource$", new Fields("word"));
		builder.setBolt("$count$", new RollingCountBolt(60, time), 2)
		.fieldsGrouping("$datasource$", new Fields("merchandiseIDS"));
		builder.setBolt("$rank$", new RankBolt(TOP_N), 2)
				.fieldsGrouping("$count$", new Fields("merchandiseID"));
		builder.setBolt("$merge$", new MergeBolt(TOP_N))
				.globalGrouping("$rank$");
		builder.setBolt("$Merchandise2UserBolt$", new Merchandise2UserBolt())
				.globalGrouping("$datasource$");
		builder.setBolt("$User2MerchandiseBolt$", new User2MerchandiseBolt())
				.globalGrouping("$datasource$");
		
//		Config conf = new Config();
//		conf.setDebug(true);
//		System.out.println("-------------start-------------------");
//		LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("Test10", conf, builder.createTopology());
//		try {
//			Thread.sleep(5000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		cluster.shutdown();
//		
		
		Config conf = new Config();
//		conf.setDebug(true);
		conf.setNumWorkers(2);
		conf.setMaxSpoutPending(5000);
		
		StormSubmitter.submitTopology("TOP-N", conf, builder.createTopology());
		
		Thread.sleep(5000);
	}
}




