package com.syx.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

/**
 * 使用coprocessor框架的索引regionObserver实现<br>
 * 
 * @author yongxingshao
 *
 */
public class SecondaryIndexer extends BaseRegionObserver {
	private static final Logger LOG = Logger.getLogger(SecondaryIndexer.class);
		
	private byte[] columnFamily;
	private String rowkeySplitor;
	private String rowkeyColumns;
	private String indexColumns; 
	
	private HttpSolrServer solrServer; 
	private int maxAddDocumentsCount;
	
	private List<SolrInputDocument> documents = new ArrayList<SolrInputDocument>();
	
	private Timer timer;
	
	public SecondaryIndexer() {
		LOG.info("init index regionObserver...");
	}
	
	@Override
	public void start(CoprocessorEnvironment coprocessorenvironment) throws IOException {
		LOG.info("start index coprocessor...");
	}

	@Override
	public void postOpen(ObserverContext<RegionCoprocessorEnvironment> observerContext) {
		HTableDescriptor tableDescriptor = observerContext.getEnvironment().getRegion().getTableDesc();
		
		LOG.info("start a timer for auto committing solr documents...");
		timer = new Timer(true);
		timer.schedule(new autoCommitDocuemtsTimerTask(), 
				0, 
				Long.parseLong(tableDescriptor.getValue(PropertyName.AUTO_ADD_COMMIT_DOCUMENTS_INTERVAL_ATTR)));
		
		String solr_server_url = tableDescriptor.getValue(PropertyName.SOLR_SERVER_URL_ATTR) + "/" + tableDescriptor.getValue(PropertyName.SOLR_COLLECTION_ATTR);
		solrServer = new HttpSolrServer(solr_server_url);
		maxAddDocumentsCount = Integer.parseInt(tableDescriptor.getValue(PropertyName.MAX_ADD_DOCUMENTS_COUNT_ATTR));
		
		columnFamily = tableDescriptor.getColumnFamilies()[0].getName();
		rowkeySplitor = tableDescriptor.getValue(PropertyName.ROW_KEY_SPLITOR_ATTR);
		rowkeyColumns = tableDescriptor.getValue(PropertyName.ROW_KEY_COLUMNS_ATTR);
		indexColumns = tableDescriptor.getValue(PropertyName.INDEX_CLOUMNS_ATTR);
	}
	
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> observerContext,
			Put put, WALEdit edit, boolean writeToWAL) throws IOException {
		SolrInputDocument solrDoc = buildDocument(put, indexColumns);
		addDocument(solrDoc);
    	addDocumnts(documents);
	}
	
	@Override
	public void postClose(
			ObserverContext<RegionCoprocessorEnvironment> observercontext,
			boolean flag) {
		addDocumntsBeforeClose();
		getCommitThread().start();
		
		LOG.info("stop the auto commit solr documents timer");
		timer.cancel();
		timer.purge();
	}
	
	private SolrInputDocument buildDocument(Put put, String indexColumns) {
		SolrInputDocument solrDoc = new SolrInputDocument();
		
		// index rowkeys
		String rowKey = new String(put.getRow()); 
    		solrDoc.addField("id", rowKey);
    		String[] rowkeyColumnNames = rowkeyColumns.split(",");
    		String[] rowkeyColumnValues = rowKey.split(rowkeySplitor);
    		for (int i=0; i<rowkeyColumnNames.length; i++) {
    			solrDoc.addField(rowkeyColumnNames[i], rowkeyColumnValues[i]);
    		} 
    	
    		// index other columns
    		String[] columns = indexColumns.split(",");
    		for (String column : columns) {
    			List<KeyValue> keyValueList = put.get(columnFamily, Bytes.toBytes(column));
    			if (keyValueList != null && keyValueList.size() > 0) {
    				String fieldName = new String(keyValueList.get(0).getQualifier());
    				String fieldValue = new String(keyValueList.get(0).getValue());
    			
    				solrDoc.addField(fieldName, fieldValue);
    			}
    		}
    	
    		return solrDoc;
	}
	
	private synchronized void addDocument(SolrInputDocument solrDoc) {
		documents.add(solrDoc);  
	}
	
	private synchronized void addDocumnts(List<SolrInputDocument> documents) {
		if (documents != null && documents.size() >= maxAddDocumentsCount) {
			try {
				solrServer.add(documents);
				LOG.info("solr add, add size: " + documents.size());
				documents.clear();
			} catch (Exception e) {
				LOG.error("add fail", e);
				e.printStackTrace();
				retryAddDocumnts(false);
			}
		}
	}
	
	private synchronized void addDocumntsBeforeClose() {
		if (documents != null && !documents.isEmpty()) {
			try {
				solrServer.add(documents);
				LOG.info("add before close region, add size: " + documents.size());
				documents.clear();
			} catch (Exception e) {
				LOG.error("add fail", e);
				retryAddDocumnts(false);
			}
		} else {
			LOG.info("no cached docs, close region directly.");
		}
	}
	
	private synchronized void autoAddDocuments() {
		if (documents != null && !documents.isEmpty()) {
			try {
				solrServer.add(documents);
				LOG.info("auto add, add size: " + documents.size());
				documents.clear();
			} catch (Exception e) {
				LOG.error("add fail", e);
				e.printStackTrace();
				retryAddDocumnts(true);
			}
		}
	}
	
	private void retryAddDocumnts(boolean autoCommit) {
		for (int i=1; i<=3; i++) {
			try {
				solrServer.add(documents);
				documents.clear();
				break;
			} catch (Exception e) {
				LOG.error("add retry " + i + "times fail: ", e);
				e.printStackTrace();
				if (i == 3) {
					LOG.error("add retry many times fail, please check the solr server's status.");
					e.printStackTrace();
				}
			}
		}
	}
	
	class autoCommitDocuemtsTimerTask extends TimerTask {
		@Override
		public void run() {
			autoAddDocuments();
			getCommitThread().start();
		}
	}
	
	private Thread getCommitThread() {
		return new Thread() {
			@Override
			public void run() {
				try {
					solrServer.commit();
				} catch (SolrServerException e) {
					LOG.error("solr commit failed--solr server error", e);
					e.printStackTrace();
				} catch (IOException e) {
					LOG.error("solr commit failed--io error", e);
					e.printStackTrace();
				}
			}
		};
	}
	
	interface PropertyName {
		//solr server config attrs
		static final String SOLR_SERVER_URL_ATTR = "solr_server_url";
		static final String MAX_ADD_DOCUMENTS_COUNT_ATTR = "max_add_documents_count";
		static final String AUTO_ADD_COMMIT_DOCUMENTS_INTERVAL_ATTR = "auto_add_commit_documents_interval";
		
		//index table config attrs
		static final String ROW_KEY_SPLITOR_ATTR = "rowkeySplitor";
		static final String ROW_KEY_COLUMNS_ATTR = "rowkeyColumns";
		static final String INDEX_CLOUMNS_ATTR = "indexColumns";
		static final String SOLR_COLLECTION_ATTR = "solrCollection";
	}
}
