package com.clarity.storm.kafkastorm;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTableConnector implements Serializable {

	private static final long serialVersionUID = -7590901166332127432L;

	private static final Logger logger = LoggerFactory.getLogger(HTableConnector.class);

	private Configuration conf;
	protected HTable table;
	protected String tableName;

	/**
	 * Initialize HTable connection
	 * 
	 * @param conf
	 *            The {@link TupleTableConfig}
	 * @throws IOException
	 */
	public HTableConnector(final TupleTableConfig conf) throws IOException {
		this.tableName = conf.getTableName();
		this.conf = HBaseConfiguration.create();
		this.conf.set("hbase.rootdir", "hdfs://10.1.10.160:8020/apps/hbase/data");
		this.conf.set("hbase.master", "10.1.10.160" + ":" + "8020");
		this.conf.set("hbase.zookeeper.quorum", "10.1.10.160");
		this.conf.set("hbase.zookeeper.property.clientPort", "2181");
		this.conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		this.conf.set("hbase.connection.timeout", "12000");
        HConnection connection = HConnectionManager.getConnection(this.conf);
		//HBaseAdmin.checkHBaseAvailable(conf);
        logger.info("HTableConnector config = " + connection.getConfiguration());
	
		try {
			 logger.info("HTableConnector creating table*****");
			this.table = new HTable(this.conf, this.tableName);
			logger.info("HTableConnector table obtained");
		} catch (IOException ex) {
			throw new IOException(
					"Unable to establish connection to HBase table "
							+ this.tableName, ex);
		}

		if (conf.isBatch()) {
			// Enable client-side write buffer
			this.table.setAutoFlush(false, true);
			logger.info("Enabled client-side write buffer");
		}

		// If set, override write buffer size
		if (conf.getWriteBufferSize() > 0) {
			try {
				this.table.setWriteBufferSize(conf.getWriteBufferSize());

				logger.info("Setting client-side write buffer to "
						+ conf.getWriteBufferSize());
			} catch (IOException ex) {
				logger.error(
						"Unable to set client-side write buffer size for HBase table "
								+ this.tableName, ex);
			}
		}

		// Check the configured column families exist
		for (String cf : conf.getColumnFamilies()) {
			if (!columnFamilyExists(cf)) {
				throw new RuntimeException(String.format(
						"HBase table '%s' does not have column family '%s'",
						conf.getTableName(), cf));
			}
		}
	}

	/**
	 * Checks to see if table contains the given column family
	 * 
	 * @param columnFamily
	 *            The column family name
	 * @return boolean
	 * @throws IOException
	 */
	private boolean columnFamilyExists(final String columnFamily)
			throws IOException {
		return this.table.getTableDescriptor().hasFamily(
				Bytes.toBytes(columnFamily));
	}

	/**
	 * @return the table
	 */
	public HTable getTable() {
		return table;
	}

	/**
	 * Close the table
	 */
	public void close() {
		try {
			this.table.close();
		} catch (IOException ex) {
			logger.error("Unable to close connection to HBase table " + tableName,
					ex);
		}
	}

}
