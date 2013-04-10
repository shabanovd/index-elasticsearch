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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.exist.dom.ElementImpl;
import org.exist.dom.QName;
import org.exist.storage.ElementValue;
import org.exist.storage.NodePath;
import org.exist.util.XMLString;
import org.exist.xquery.XPathException;
import org.exist.xquery.value.Type;
import org.w3c.dom.Element;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class RecordMatcher {
	
	public static final String index = "exist-db";
	public static final String type = "node";

	NodePath path;
	Set<Field> fields = new HashSet<Field>();
	
	public RecordMatcher(String sPath, Map<String, String> namespaces) {
		path = new NodePath( namespaces, sPath, false );
	}

	public boolean match(NodePath otherPath) {
		return path.match(otherPath);
	}

	public Field matchField(NodePath recordPath, NodePath path) {
		for (Field field : fields)
			if (field.match(path, recordPath.length())) 
				return new Field(field);
		
		return null;
	}
//
//	public Field matchAttribute(QName name) {
//		for (Field field : fields)
//			if (field.matchAttribute(name)) return field;
//		
//		return null;
//	}

	public void addField(Element elem, Map<String, String> namespaces) throws XPathException {
		String sPath = elem.getAttribute(ESConfig.PATH_ATTR);
		String sType = elem.getAttribute(ESConfig.TYPE_ATTR);

		fields.add(new Field(namespaces, sPath, sType));
	}
	
	class Field {
		NodePath _path;
		int _type;
		
		StringBuilder sb = null;
		
		public Field(Map<String, String> namespaces, String path, String type) throws XPathException {
			boolean includeDescendants = false;
			if (type.isEmpty()) {
				_type = Type.STRING;
			} else {
				_type = Type.getType(type);
			}
			_path = new NodePath( namespaces, path, includeDescendants );
		}

		public Field(Field field) {
			_path = field._path;
			_type = field._type;

			sb = new StringBuilder();
		}

		public boolean match(NodePath path, int offset) {
			return _path.match(path, offset);
		}

		public boolean matchAttribute(QName name) {
			if (_path.length() == 1) {
				QName comp = _path.getComponent(0);
				return (comp.equalsSimple(name) && comp.getNameType() == ElementValue.ATTRIBUTE);
			}
			return false;
		}
		
		public void characters(String string) {
			if (sb.length() != 0) 
				sb.append(" ");
			
			sb.append(string);
		}

		public void characters(XMLString xmlString) {
			if (sb.length() != 0) 
				sb.append(" ");
			
			sb.append(xmlString);
		}

		public String getValue() {
			return sb.toString();
		}

		ElementImpl _element = null;
		public void setElement(ElementImpl element) {
			_element = element;
		}
		public ElementImpl getElement() {
			return _element;
		}
	}
}
