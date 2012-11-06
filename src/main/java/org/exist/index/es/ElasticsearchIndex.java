/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2012 The eXist Project
 *  http://exist-db.org
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public License
 *  as published by the Free Software Foundation; either version 2
 *  of the License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 *  $Id$
 */
package org.exist.index.es;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.exist.backup.RawDataBackup;
import org.exist.indexing.AbstractIndex;
import org.exist.indexing.IndexWorker;
import org.exist.indexing.RawBackupSupport;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.storage.btree.DBException;
import org.exist.util.DatabaseConfigurationException;
import org.w3c.dom.Element;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class ElasticsearchIndex extends AbstractIndex implements RawBackupSupport {
	
	static {
    	ID = ElasticsearchIndex.class.getName();
	}
	
    protected static final Logger LOG = Logger.getLogger(ElasticsearchIndex.class);

    private String dataDir;
    protected Node node;

    public ElasticsearchIndex() {
    }

    @Override
    public void configure(BrokerPool pool, String dataDir, Element config) throws DatabaseConfigurationException {
        super.configure(pool, dataDir, config);
        
        this.dataDir = new File(dataDir, "es").toString();
    }

    @Override
    public void open() throws DatabaseConfigurationException {
		Builder finalSettings = settingsBuilder()
				.put("node.local", true)
				.put("gateway.type", "local")
				.put("path.home", dataDir)
				.put("index.number_of_shards", 2).put("index.number_of_replicas", 1)
				.put("name", "eXist-db")
				.put("cluster.name", "eXist-db-cluster");

		node = NodeBuilder.nodeBuilder().settings(finalSettings).node();
		AdminClient admin = getClient().admin();

		ClusterHealthResponse health = admin.cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        if (health.isTimedOut())
        	System.out.println("!!! Time out for yellow !!!");

        ClusterHealthResponse clusterHealth = admin.cluster().health(clusterHealthRequest()).actionGet();
        System.out.println("Done Cluster Health, status " + clusterHealth.status());
        if (clusterHealth.timedOut()) {
        	String msg = "Failed to initialize elasticsearch index: time out health status fetch";
            LOG.error(msg);
            throw new DatabaseConfigurationException(msg);
        }
        if (clusterHealth.status().value() > ClusterHealthStatus.YELLOW.value()) {
        	String msg = "Failed to initialize elasticsearch index: status = "+clusterHealth.status();
            LOG.error(msg);
            throw new DatabaseConfigurationException(msg);
        }
        
		IndicesExistsResponse res = admin.indices().prepareExists(RecordMatcher.index).execute().actionGet();
		if (!res.exists()) {
			try {
				admin.indices()
					.prepareCreate(RecordMatcher.index)
					.execute().actionGet();

			} catch (ElasticSearchException e) {
				e.printStackTrace();
			}
		}
		
        System.out.println("Up & running ...");
    }

    @Override
    public void close() throws DBException {
    	node.close();
    	node = null;
    }

    @Override
    public void sync() throws DBException {
		Client client = getClient();
		try {
			RefreshResponse responce = client.admin().indices().prepareRefresh().execute().actionGet();
			if (responce.failedShards() > 0)
				LOG.info("failedShards "+responce.failedShards()+" during sync call.");
		} finally {
			client.close();
		}
    }
    
    public Client getClient() {
    	return node.client();
    }

    @Override
    public void remove() throws DBException {
    }

    @Override
    public IndexWorker getWorker(DBBroker broker) {
        return new ESIndexWorker(this, broker);
    }

    @Override
    public boolean checkIndex(DBBroker broker) {
        return false;
    }

	@Override
	public void backupToArchive(RawDataBackup backup) throws IOException {
		//XXX: think how?
//        OutputStream os = backup.newEntry(btree.getFile().getName());
//        btree.backupToStream(os);
//        backup.closeEntry();
	}
}
