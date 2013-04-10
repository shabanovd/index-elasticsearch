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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Hex;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.exist.Database;
import org.exist.dom.AttrImpl;
import org.exist.dom.ElementImpl;
import org.exist.index.es.RecordMatcher.Field;
import org.exist.storage.NodePath;
import org.exist.util.XMLString;
import org.exist.xquery.XPathException;
import org.exist.xquery.value.DateTimeValue;
import org.exist.xquery.value.Type;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class Record {
	
	public static final String DOCUMENT_PATH = "_exist-document-path";
	public static final String DOCID_NODEID = "_exist-docid-nodeid";
	
	RecordMatcher matcher;
	NodePath recordPath;
	
	Set<Field> active = new HashSet<Field>();
	
	Map<String, Object> map = new HashMap<String, Object>();
	
	public Record(RecordMatcher matcher, NodePath path) {
		this.matcher = matcher;
		recordPath = new NodePath(path);
	}

	public void store(Client client, String docPath, ElementImpl el) {
		if (map.isEmpty()) 
			return;
		
		try {
			String id = Hex.encodeHexString(
					Utils.encodeDocNodeIds(el.getDocument().getCollection().getId(), el.getDocId(), el.getNodeId())
				);
			
			XContentBuilder rec = XContentFactory.jsonBuilder().startObject();
			for (Entry<String, Object> entry : map.entrySet()) {
				rec = rec.field(entry.getKey(), entry.getValue());
				System.out.println(entry.getKey()+" "+entry.getValue());
			}
			rec = rec.field(DOCUMENT_PATH, docPath);
//			rec = rec.field(DOCID_NODEID, );
			rec = rec.endObject();
			
//			IndexResponse response = 
			client
				.prepareIndex(RecordMatcher.index, RecordMatcher.type)
				.setId(id)
				.setSource(rec)
				.execute().actionGet();
			
//			if (response. hasFailures()) {
//				System.out.println(response.getId());
//			}
		} catch (Exception e) {
			//XXX: log
			e.printStackTrace();
		}
	}
	
	public static void delete(Node node, long id) {
		Client client = node.client();
		
		IndicesExistsResponse res = client.admin().indices().prepareExists(RecordMatcher.index).execute().actionGet();
		if (!res.exists()) {
			return;
		}
		
		try {
			client.delete(new DeleteRequest(RecordMatcher.index, RecordMatcher.type, Long.toHexString(id))).actionGet();
		} catch (Exception e) {
			//XXX: log
			e.printStackTrace();
		} finally {
			client.close();
		}
	}

	public void element(NodePath path, ElementImpl element) {
		Field field = matcher.matchField(recordPath, path);
		if (field != null) {
			field.setElement(element);
			active.add(field);
			System.out.println("MATCHED "+field._path);
		}
	}

	public void attribute(Database db, NodePath path, AttrImpl attr) {
		NodePath _path_ = new NodePath(path);
		_path_.addComponent(attr.getQName());
		
		Field field = matcher.matchField(recordPath, _path_);
		if (field != null) {
			field.characters(attr.getValue());
			add(db, field);
		}
	}

	public void characters(XMLString xmlString) {
		for (Field field : active) {
			field.characters(xmlString);
		}
	}

	public void endElement(Database db, NodePath path, ElementImpl element) {
		Iterator<Field> it = active.iterator();
		while (it.hasNext()) {
			Field field = it.next(); 
			if (field.getElement() == element) {
				add(db, field);
				it.remove();
			}
		}
	}

	public void add(Database db, Field field) {
		Object obj;
		if (Type.subTypeOf(field._type, Type.DATE_TIME)) {
			try {

				obj = (new DateTimeValue(field.getValue())).toJavaObject(Date.class);
			
			} catch (XPathException e) {
				//XXX: log
				e.printStackTrace();
				return;
			}
		} else {
			//default xs:string
			obj = field.getValue();
		}

		final String key = field._path.toString();
		Object _obj_ = map.get(key);
		if (_obj_ == null)
			map.put(key, obj);

		else if (_obj_ instanceof List) {
			List<Object> list = (List<Object>) _obj_;
			list.add(obj);
		
		} else {
			List<Object> list = new ArrayList<Object>();
			list.add(_obj_);
			list.add(obj);
			map.put(key, list);
		}
	}
}
