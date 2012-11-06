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

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.exist.Database;
import org.exist.collections.Collection;
import org.exist.dom.*;
import org.exist.indexing.*;
import org.exist.storage.*;
import org.exist.storage.txn.Txn;
import org.exist.util.*;
import org.exist.xquery.*;
import org.w3c.dom.*;

import java.util.*;

import org.exist.security.PermissionDeniedException;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class ESIndexWorker implements IndexWorker {

    private ElasticsearchIndex index;
    
    private DBBroker broker;

    private int mode = 0;
    private DocumentImpl document;
    private String docPath;
    private ESConfig config;
    
	public ESIndexWorker(ElasticsearchIndex index, DBBroker broker) {
        this.index = index;
        this.broker = broker;
    }
	
	public Database getDatabase() {
		return broker.getDatabase();
	}

	public Client getClient() {
		return index.getClient();
	}
	
    public String getIndexId() {
        return index.getIndexId();
    }

    public String getIndexName() {
        return index.getIndexName();
    }

    @Override
	public Object configure(IndexController controller, NodeList configNodes, 
			Map namespaces) throws DatabaseConfigurationException {
        config = new ESConfig(configNodes, namespaces);
		return config;
	}

	public void setDocument(DocumentImpl doc) {
        setDocument(doc, StreamListener.UNKNOWN);
    }

    public void setDocument(DocumentImpl doc, int mode) {
    	config = null;
    	
        this.document = doc;
        docPath = document.getURI().toString();
        this.mode = mode;
        
        IndexSpec indexConf = document.getCollection().getIndexConfiguration(broker);
        if (indexConf != null) {
            config = (ESConfig) indexConf.getCustomIndexSpec(ElasticsearchIndex.getID());
        }
    }

    public void setMode(int mode) {
        this.mode = mode;
    }

    public DocumentImpl getDocument() {
        return document;
    }

    public int getMode() {
        return mode;
    }

    public StoredNode getReindexRoot(StoredNode node, NodePath path, boolean includeSelf) {
        return node;
    }

//    private ElasticsearchStreamListener listener = new ElasticsearchStreamListener();
    
    public StreamListener getListener() {
        return new ElasticsearchStreamListener();
    }

    public MatchListener getMatchListener(DBBroker broker, NodeProxy proxy) {
        // not applicable to this index
        return null;
    }

    public void flush() {
        switch (mode) {
            case StreamListener.STORE:
                processPending();
                break;
            case StreamListener.REMOVE_ALL_NODES:
                removeDocument(document);
                break;
            case StreamListener.REMOVE_SOME_NODES:
                removeSome();
        }
    }
    
	protected void removeSome() {
    }

    protected void removeDocument(DocumentImpl docToRemove) {
    	getClient().prepareDeleteByQuery(RecordMatcher.index)
    		.setQuery(QueryBuilders.termQuery(Record.DOCUMENT_PATH, docToRemove.getURI().toString()))
    		.execute().actionGet();
    }

    @Override
    public void removeCollection(Collection collection, DBBroker broker) throws PermissionDeniedException {
        for (Iterator<DocumentImpl> i = collection.iterator(broker); i.hasNext(); ) {
            DocumentImpl doc = i.next();
            removeDocument(doc);
        }
    }

    public boolean checkIndex(DBBroker broker) {
        return false;
    }

    public Occurrences[] scanIndex(XQueryContext context, DocumentSet docs, NodeSet contextSet, Map hints) {
        return new Occurrences[0];
    }

    private void processPending() {
    	;
    }

    List<Record> records = null;

    private class ElasticsearchStreamListener extends AbstractStreamListener {
    	
    	Stack<List<Record>> stack = new Stack<List<Record>>();
    	Set<Record> active = new HashSet<Record>();

    	Client client = getClient();
        
    	private ElasticsearchStreamListener() {
        }

        @Override
        public void startElement(Txn transaction, ElementImpl element, NodePath path) {
            super.startElement(transaction, element, path);
            if (mode == StreamListener.REMOVE_ALL_NODES) {
            } else if (mode == StreamListener.STORE || mode == StreamListener.REMOVE_SOME_NODES) {
            	if (config != null) {
                	if (!active.isEmpty()) {
                		for (Record record : active) {
                			record.element(path, element);
                		}
                	}

                	records = config.getRecords(path);
                	stack.push(records);
                	active.addAll(records);
            	}
            }
        }

        @Override
        public void endElement(Txn transaction, ElementImpl element, NodePath path) {
            super.endElement(transaction, element, path);
            
            if (mode == StreamListener.REMOVE_ALL_NODES) {
            	Record.delete(index.node, element.getInternalAddress());
            	
            	//UNDERSTAND: what to do with REMOVE_SOME_NODES?
            	
            } else if (mode == StreamListener.STORE || mode == StreamListener.REMOVE_SOME_NODES) {
            	if (config != null) {
	            	records = stack.pop();
		            for (Record record : records) {
			            if (record != null) {
			            	 record.store(
		            			 client, 
		            			 docPath, 
		            			 element 
	            			 );
			            }
			            active.remove(record);
		            }
		            
	            	if (config != null) {
			        	if (!active.isEmpty()) {
			        		for (Record record : active) {
			        			record.endElement(getDatabase(), path, element);
			        		}
			        	}
	            	}
		            
            	}
            }
        }

        @Override
        public void attribute(Txn transaction, AttrImpl attr, NodePath path) {
            super.attribute(transaction, attr, path);

            if (mode == StreamListener.STORE || mode == StreamListener.REMOVE_SOME_NODES) {
            	if (config != null) {
	            	records = stack.peek();
		            for (Record record : records) {
		            	record.attribute(getDatabase(), path, attr);
		            }
            	}
            }
        }

        @Override
        public void characters(Txn transaction, CharacterDataImpl text, NodePath path) {
            if (mode == StreamListener.STORE || mode == StreamListener.REMOVE_SOME_NODES) {
            	if (config != null) {
		        	if (!active.isEmpty()) {
		        		for (Record record : active) {
		        			record.characters(text.getXMLString());
		        		}
		        	}
            	}
        	}
            super.characters(transaction, text, path);
        }
        
        @Override
        public IndexWorker getWorker() {
            return ESIndexWorker.this;
        }
    }

	public org.elasticsearch.node.Node getNode() {
		return index.node;
	}
}