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

import org.exist.Database;
import org.exist.dom.DocumentImpl;
import org.exist.dom.QName;
import org.exist.dom.SymbolTable;
import org.exist.numbering.NodeId;
import org.exist.util.ByteConversion;
import org.exist.xquery.XPathException;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.NumericValue;
import org.exist.xquery.value.Type;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class Utils {

	public static int getInt(Item item) throws XPathException {
    	NumericValue value;
    	if (item instanceof NumericValue) {
			value = (NumericValue) item;
		} else {
			value = (NumericValue) item.convertTo(Type.NUMBER);
		}
    	return value.getInt();
	}

	public static long getLong(Item item) throws XPathException {
    	NumericValue value;
    	if (item instanceof NumericValue) {
			value = (NumericValue) item;
		} else {
			value = (NumericValue) item.convertTo(Type.NUMBER);
		}
    	return value.getLong();
	}

	
	public static String encodeQName(Database db, QName qname) {
        SymbolTable symbols = db.getSymbols();
        short namespaceId = symbols.getNSSymbol(qname.getNamespaceURI());
        short localNameId = symbols.getSymbol(qname.getLocalName());
        long nameId = qname.getNameType() | (namespaceId & 0xFFFF) << 16 | (localNameId & 0xFFFFFFFFL) << 32;
        return Long.toHexString(nameId);
    }

	public static QName decodeQName(Database db, String s) {
        SymbolTable symbols = db.getSymbols();
        try {
            long l = Long.parseLong(s, 16);
            short namespaceId = (short) ((l >>> 16) & 0xFFFFL);
            short localNameId = (short) ((l >>> 32) & 0xFFFFL);
            byte type = (byte) (l & 0xFFL);
            String namespaceURI = symbols.getNamespace(namespaceId);
            String localName = symbols.getName(localNameId);
            QName qname = new QName(localName, namespaceURI, "");
            qname.setNameType(type);
            return qname;
        } catch (NumberFormatException e) {
            return null;
        }
    }

	private static int LENGTH_INT = 4;
	
	private static int colDocShift = LENGTH_INT + DocumentImpl.LENGTH_DOCUMENT_ID;
	private static int prefixLength = colDocShift + NodeId.LENGTH_NODE_ID_UNITS;
	
	public static byte[] encodeDocNodeIds(int colId, int docId, NodeId nodeId) {
    	int len = prefixLength + nodeId.size();
    	byte[] data = new byte[len];
        
        ByteConversion.intToByteH(colId, data, 0);
        ByteConversion.intToByteH(docId, data, LENGTH_INT);
        ByteConversion.shortToByteH((short) nodeId.units(), data, colDocShift);

        nodeId.serialize(data, prefixLength);
        
        return data;
	}

	public static int decodeColId(byte[] data) {
        return ByteConversion.byteToIntH(data, 0);
	}

	public static int decodeDocId(byte[] data) {
        return ByteConversion.byteToIntH(data, LENGTH_INT);
	}

	public static NodeId decodeNodeId(Database db, byte[] data) {
        short units = ByteConversion.byteToShortH(data, colDocShift);
    	return db.getNodeFactory().createFromData(units, data, prefixLength);
    }
}
