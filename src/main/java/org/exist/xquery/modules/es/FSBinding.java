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

import org.exist.dom.QName;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.value.SequenceType;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class FSBinding extends FunctionSignature {

	Class<?> _clazz;
	BindingMethodsAnalizer _method;
	
	public FSBinding(Class<?> clazz, BindingMethodsAnalizer method, QName name, SequenceType[] arguments, SequenceType returnType, boolean overloaded) {
		super(name, arguments, returnType, overloaded);
		_clazz = clazz;
		_method = method;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(_clazz).append(" . ").append(_method).append("\n");
		sb.append(super.toString());
		return sb.toString();
	}
}
