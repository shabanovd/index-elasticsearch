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
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.exist.storage.NodePath;
import org.exist.util.DatabaseConfigurationException;
import org.exist.xquery.XPathException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class ESConfig {
	
	private final static Logger LOG = Logger.getLogger(ESConfig.class);

    private final static String CONFIG_ROOT = "es";
    private final static String RECORD = "record";
    private final static String FIELD = "field";
    protected final static String PATH_ATTR = "path";
    protected final static String TYPE_ATTR = "type";
    
    private List<RecordMatcher> matchers = new ArrayList<RecordMatcher>();

	public ESConfig(NodeList configNodes, Map<String, String> namespaces) {
        try {
        	parseConfig(configNodes, namespaces, null, false);

        } catch (DatabaseConfigurationException e) {
			LOG.warn("Invalid ES configuration element: " + e.getMessage());
        } catch (XPathException e) {
			LOG.warn("Invalid ES configuration element: " + e.getMessage());
		}
	}

	private void parseConfig(NodeList configNodes, Map<String, String> namespaces, RecordMatcher matcher, boolean underES) throws DatabaseConfigurationException, XPathException {
        Node node = null;
        for(int i = 0; i < configNodes.getLength(); i++) {
            node = configNodes.item(i);
            if(node.getNodeType() == Node.ELEMENT_NODE) {
				if (CONFIG_ROOT.equals(node.getLocalName())) {
				    parseConfig(node.getChildNodes(), namespaces, matcher, true);
				
				} else  if (RECORD.equals(node.getLocalName())) {
					if (matcher != null) {
			            throw new DatabaseConfigurationException("Record can't be inside another record.");
					}
					
				    Element elem = (Element) node;
				    if (!elem.hasAttribute(PATH_ATTR)) {
			            throw new DatabaseConfigurationException("Record must have 'path' attribute.");
				    }
					
				    matcher = new RecordMatcher(elem.getAttribute(PATH_ATTR), namespaces);
				    parseConfig(node.getChildNodes(), namespaces, matcher, underES);
				    matchers.add(matcher);
				    matcher = null;
				
				} else  if (FIELD.equals(node.getLocalName())) {
					if (matcher == null) {
			            throw new DatabaseConfigurationException("Field must be under Record.");
					}
					
				    Element elem = (Element) node;
				    if (!elem.hasAttribute(PATH_ATTR)) {
			            throw new DatabaseConfigurationException("Record must have 'path' attribute.");
				    }
				    matcher.addField(elem, namespaces);
				
				} else if (underES) {
					LOG.warn("Unknown configuration element: " + node.getLocalName());
				}
	        }
        }
	}

	public List<Record> getRecords(NodePath path) {
		List<Record> ms = new ArrayList<Record>();
		for (RecordMatcher matcher : matchers) {
			if (matcher.match(path))
				ms.add(new Record(matcher, path));
		}
		return ms;
	}

}
