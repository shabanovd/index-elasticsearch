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
package org.exist.xquery.modules.es;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Stack;

import org.elasticsearch.common.Hex;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentString;
import org.elasticsearch.common.xcontent.XContentType;
import org.exist.Database;
import org.exist.dom.DocumentImpl;
import org.exist.dom.DocumentSet;
import org.exist.dom.NodeProxy;
import org.exist.dom.QName;
import org.exist.index.es.Utils;
import org.exist.memtree.DocumentBuilderReceiver;
import org.xml.sax.SAXException;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class XmlXContentGenerator implements XContentGenerator {

	Database db;
	
	DocumentBuilderReceiver receiver;
	DocumentSet docs;
	
	QName openTag = null;
	
	Stack<QName> startedObject = new Stack<QName>();
	Stack<QName> startedArray = new Stack<QName>();
	
	String field = null;
	
	boolean underHits = false;
	
	public XmlXContentGenerator(Database db, DocumentBuilderReceiver receiver, DocumentSet docs) {
		this.db = db;
		this.receiver = receiver;
		this.docs = docs;
	}

	public XContentType contentType() {
		return XContentType.JSON;
	}

	public void usePrettyPrint() {
		throw new RuntimeException("unimplemented method");
	}

	public void writeStartArray() throws IOException {
		startedArray.push(openTag);
		
		if (openTag.getLocalName().equals("hits"))
			underHits = true;
		
		openTag = null;
	}

	public void writeEndArray() throws IOException {
		openTag = startedArray.pop();
		
		if (openTag != null && openTag.getLocalName().equals("hits"))
			underHits = false;
	}

	public void writeStartObject() throws IOException {
		System.out.println("writeStartObject() "+openTag);
		startedObject.push(openTag);
		openTag = null;
	}

	public void writeEndObject() throws IOException {
		try {
			openTag = startedObject.pop();
			System.out.println("writeEndObject() "+openTag);
			if (openTag != null) {
				receiver.endElement(openTag);
				openTag = null;
			}
		} catch (SAXException e) {
			throw new IOException(e);
		}
	}

	public void writeFieldName(String name) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeFieldName(XContentString name) throws IOException {
		System.out.println(name.getValue());
		if (underHits) {
			field = name.getValue();
			return;
		}
		
		try {
			if (openTag != null) {
				receiver.endElement(openTag);
			
				openTag = null;
			}
		
			openTag = new QName(name.getValue(), Module.NAMESPACE_URI, Module.PREFIX);
			
			receiver.startElement(openTag, null);
		} catch (SAXException e) {
			throw new IOException(e);
		}
	}

	public void writeString(String text) throws IOException {
		System.out.println("writeString "+text);
		if (field != null && "_id".equals(field)) {
			
			try {
				byte[] data = Hex.decodeHex(text);
			
				DocumentImpl doc = docs.getDoc(Utils.decodeDocId(data));
				if (doc != null) {
					receiver.addReferenceNode( new NodeProxy(doc, Utils.decodeNodeId(db, data)) );
				}
			} catch (SAXException e) {
				throw new IOException(e);
			} catch (Exception e) {
				//XXX: log
			}
		}
	}

	public void writeString(char[] text, int offset, int len) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeUTF8String(byte[] text, int offset, int length) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeBinary(byte[] data, int offset, int len) throws IOException {
		System.out.println("writeBinary(byte[] data, int offset, int len)");
	}

	public void writeBinary(byte[] data) throws IOException {
		System.out.println("writeBinary(byte[] data)");
		writeBinary(data, 0, data.length);
	}

	public void writeNumber(String v) throws IOException {
		if (underHits)
			return;
		
		try {
			receiver.characters(v);
		} catch (SAXException e) {
			throw new IOException(e);
		}
	}

	public void writeNumber(int v) throws IOException {
		writeNumber(String.valueOf(v));
	}

	public void writeNumber(long v) throws IOException {
		writeNumber(String.valueOf(v));
	}

	public void writeNumber(double d) throws IOException {
		writeNumber(String.valueOf(d));
	}

	public void writeNumber(float f) throws IOException {
		writeNumber(String.valueOf(f));
	}

	public void writeBoolean(boolean state) throws IOException {
		try {
			receiver.characters(state ? "true" : "false");
		} catch (SAXException e) {
			throw new IOException(e);
		}
	}

	public void writeNull() throws IOException {
		System.out.println("writeNull");
	}

	public void writeStringField(String fieldName, String value) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeStringField(XContentString fieldName, String value) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeBooleanField(String fieldName, boolean value) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeBooleanField(XContentString fieldName, boolean value) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeNullField(String fieldName) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeNullField(XContentString fieldName) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeNumberField(String fieldName, int value) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeNumberField(XContentString fieldName, int value) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeNumberField(String fieldName, long value) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeNumberField(XContentString fieldName, long value) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeNumberField(String fieldName, double value) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeNumberField(XContentString fieldName, double value) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeNumberField(String fieldName, float value) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeNumberField(XContentString fieldName, float value) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeBinaryField(String fieldName, byte[] data) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeBinaryField(XContentString fieldName, byte[] data) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeArrayFieldStart(String fieldName) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeArrayFieldStart(XContentString fieldName) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeObjectFieldStart(String fieldName) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeObjectFieldStart(XContentString fieldName) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeRawField(String fieldName, byte[] content, OutputStream bos) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeRawField(String fieldName, byte[] content, int offset, int length, OutputStream bos) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeRawField(String fieldName, InputStream content, OutputStream bos) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void writeRawField(String fieldName, BytesReference content, OutputStream bos) throws IOException {
		System.out.println("writeRawField "+fieldName);
	}

	public void copyCurrentStructure(XContentParser parser) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void flush() throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public void close() throws IOException {
		throw new RuntimeException("unimplemented method");
	}
}
