package com.clarity.storm.kafkastorm;

import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class HBaseBolt extends BaseRichBolt {

	private static final Logger logger = LoggerFactory.getLogger(HBaseBolt.class);
	
	protected OutputCollector collector;
	protected HTableConnector connector;
	protected TupleTableConfig config;
	protected boolean ACK = true;
	private String name;
	private Fields declaredFields;
	
	public HBaseBolt(String name, TupleTableConfig config, boolean ack) {
		logger.info("************calling HBase bolt***********");
		this.name = name;
		this.config = config;	
		this.ACK = ack;
	}
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		this.connector.close();
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		logger.info("[" + this.name + "]HBaseBolt Received message: " + input);
		if(input.contains("columnData")) {
			Map<Object, Map<Object, Object>> columnData = (Map<Object, Map<Object, Object>>)input.getValueByField("columnData");
			if(columnData.size() > 0) {
				Iterator<Object> iter = columnData.keySet().iterator();
				String row_id = (String)iter.next();
				Put p = new Put(row_id.getBytes());
				Map<Object, Object> values = columnData.get(row_id);
				for (String cf : config.columnFamilies.keySet()) {
					logger.info("[" + this.name + "]ColumnFamily - " + cf);
					byte[] cfBytes = Bytes.toBytes(cf);
					for (String cq : config.columnFamilies.get(cf)) {
						logger.info("[" + this.name + "]ColumnQulaifier - " + cf);
						byte[] cqBytes = Bytes.toBytes(cq);
						Object value = values.get(cq);
						if(value != null) {
							p.add(cfBytes, cqBytes, ((String)value).getBytes());
							logger.info("[" + this.name + "]adding value to put");
						} else {
							logger.error("value is null for " + cq);
						}
					}
				}
			} else {
				logger.error("[" + this.name + "]columndata object is null");
			}
		} else {
			logger.error("[" + this.name + "]Didnt find the columnData object");
		}
		/*try {
			this.connector.getTable().put(config.getPutFromTuple(input));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}*/
		
		if(isACK()) {
			this.collector.ack(input);
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		
		try {
			this.connector = new HTableConnector(config);
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
		
		logger.info("Preparing HBaseBolt for table : " + config.getTableName());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		if(this.declaredFields != null) {
			declarer.declare(this.declaredFields);
		}
	}
	
	private boolean isACK() {
		return this.ACK;
	}
	
	private void setACK(boolean value) {
		this.ACK = value;
	}

}
