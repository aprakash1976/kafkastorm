package com.clarity.storm.kafkastorm;

import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ApacheLogParserBolt extends BaseRichBolt{

	private final Logger logger = LoggerFactory.getLogger(ApacheLogParserBolt.class);
	private Pattern p;
	private OutputCollector collector;
	private boolean ack = true;
	private Fields declaredFields;
	private String name;
	public static final String COLUMN_DATA_FIELD_NAME = "columnData";
	
	final String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" (.+)";
	
	public ApacheLogParserBolt(String name, boolean ack) {
		p = Pattern.compile(logEntryPattern);
		this.name = name;
		this.ack = ack;
		this.declaredFields = new Fields(COLUMN_DATA_FIELD_NAME);
	}
	
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		logger.info("[" + this.name + "] Received message: " + input.toString());
		//if(input.contains("id")) {
        Map<Object, Map<Object, Object>> columnData = new TreeMap<Object, Map<Object, Object>>();
		if(input.contains("\n")) {
			StringTokenizer lines = new StringTokenizer(input.toString(), "\n");
			while(lines.hasMoreTokens()) {
				Matcher matcher = p.matcher(lines.nextToken());
				if(matcher != null && matcher.groupCount() > 0) {
					Map<Object, Object> rawData = new TreeMap<Object, Object>();
					rawData.put("hostname", matcher.group(2));
					rawData.put("remoteuser", matcher.group(2));
					rawData.put("eventtimestamp", matcher.group(5));
					rawData.put("requestmethod", matcher.group(6));
					rawData.put("requeststatus", matcher.group(6));
					rawData.put("responsebytes", matcher.group(8));
					rawData.put("remotehost", matcher.group(9));
					rawData.put("agent", matcher.group(10));
					columnData.put(System.currentTimeMillis(), rawData);
				}
				Values output = new Values(columnData);
				this.collector.emit(input, output);
				this.collector.ack(input);
			}
		} else {
			if(input.toString() != null && input.toString().length() > 0) {
				String idField = (String) input.toString();
				if(idField.contains("[")) {
					idField = idField.substring(idField.indexOf("[")+1, idField.length()-2);
					logger.info("[" + this.name + "] idField message: " + idField);
				}
				Matcher matcher = p.matcher(idField.trim());
				if(matcher != null && matcher.matches()) {
					Map<Object, Object> rawData = new TreeMap<Object, Object>();
					rawData.put("hostname", matcher.group(8));
					rawData.put("remoteuser", matcher.group(1));
					rawData.put("eventtimestamp", matcher.group(4));
					rawData.put("requestmethod", matcher.group(5));
					rawData.put("requeststatus", matcher.group(6));
					rawData.put("responsebytes", matcher.group(7));
					rawData.put("remotehost", matcher.group(8));
					rawData.put("agent", matcher.group(9));
					columnData.put(System.currentTimeMillis(), rawData);
					//logger.info("[" + this.name + "] columnData: " + columnData.size());
				} else {
					logger.error("[" + this.name + "] idField match not found ");
				}
				Values output = new Values(columnData);
				this.collector.emit(input, output);
				this.collector.ack(input);
			}
		}
		//}
		
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		if(this.declaredFields != null) {
			declarer.declare(this.declaredFields);
		}
	}

}
