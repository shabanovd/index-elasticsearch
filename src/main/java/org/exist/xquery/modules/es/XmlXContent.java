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
import java.io.Reader;
import java.io.Writer;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class XmlXContent implements XContent {
	
	XContentGenerator generator;

	public XmlXContent(XContentGenerator generator) {
		this.generator = generator;
	}

	public XContentType type() {
		return XContentType.JSON;
	}

	public byte streamSeparator() {
		return '\n';
	}

	public XContentGenerator createGenerator(OutputStream os) throws IOException {
		return generator;
	}

	public XContentGenerator createGenerator(Writer writer) throws IOException {
		return generator;
	}

	public XContentParser createParser(String content) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public XContentParser createParser(InputStream is) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public XContentParser createParser(byte[] data) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public XContentParser createParser(byte[] data, int offset, int length) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public XContentParser createParser(BytesReference bytes) throws IOException {
		throw new RuntimeException("unimplemented method");
	}

	public XContentParser createParser(Reader reader) throws IOException {
		throw new RuntimeException("unimplemented method");
	}
}