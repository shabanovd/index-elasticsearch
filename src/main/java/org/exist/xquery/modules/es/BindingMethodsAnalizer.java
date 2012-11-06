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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.exist.xquery.Cardinality;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class BindingMethodsAnalizer {
	String _name;
	int _arity;
	
	protected List<Method> methods = new ArrayList<Method>();
	
	protected Class<?> _owner;
	
	Class<?>[] same = null;
	
	Class<?> returnType = null;
	
	public BindingMethodsAnalizer(Class<?> owner, String name, int arity) {
		_owner = owner;
		_name = name;
		_arity = arity;
	}
	
	public void add(Method method) {
		int arity = method.getParameterTypes().length;
		if (_arity != arity)
			throw new RuntimeException("Method arity must be "+_arity+" but get "+arity);
		
		if (returnType == null)
			returnType = method.getReturnType();
		
		assert returnType == method.getReturnType();
		
		if (same == null) {
			same = method.getParameterTypes();
		} else {
			Class<?>[] params = method.getParameterTypes();
			for (int i = 0; i < params.length; i++) {
				if (same[i] == null) {
					//it different already
					
				} else if (params[i] != same[i]) {
					//found difference
					same[i] = null;
				}
			}
		}
		
		methods.add(method);
	}

	public String getName() {
		return _name;
	}

	public Class<?> getReturnType() {
		return returnType;
	}
	
	public List<SequenceType> functionParameter() {

		System.out.println(Arrays.toString(same));

		List<SequenceType> fTypes = new ArrayList<SequenceType>();
		
		boolean error = false;
		for (Class<?> param : same) {
			int cardinality = Cardinality.EXACTLY_ONE;
			int type = Type.ATOMIC;
			
			if (param == null) {
				type = Type.ATOMIC;
				
			} else {
				Class<?> t = param;
				if (param.isArray()) {
					cardinality = Cardinality.ONE_OR_MORE;
					t = param.getComponentType();
				}
				
				if (t.isPrimitive()) {
					if (t == int.class) {
						type = Type.INTEGER;
					
					} else if (t == long.class) {
						type = Type.LONG;
					
					} else if (t == double.class) {
						type = Type.DOUBLE;
					
					} else if (t == float.class) {
						type = Type.FLOAT;
					
					} else if (t == boolean.class) {
						type = Type.BOOLEAN;
					}
				} else if (java.lang.String.class == t) {
						type = Type.STRING;
					
				} else if (t == org.elasticsearch.index.query.QueryBuilder.class) {
					type = -1;
					fTypes.add(new FunctionParameterSequenceType("query", Type.FUNCTION_REFERENCE, Cardinality.EXACTLY_ONE, ""));
				
				} else if (t == org.elasticsearch.index.query.FilterBuilder.class) {
					type = -1;
					fTypes.add(new FunctionParameterSequenceType("filter", Type.FUNCTION_REFERENCE, Cardinality.EXACTLY_ONE, ""));
				
				} else if (t == org.elasticsearch.index.query.SpanQueryBuilder.class) {
					type = -1;
					fTypes.add(new FunctionParameterSequenceType("filter", Type.FUNCTION_REFERENCE, Cardinality.EXACTLY_ONE, ""));
				
				} else {
					System.out.println("skipped "+_name+" - "+t);
					error = true;
					break;
				}
			}
			if (type > 0)
				fTypes.add(new FunctionParameterSequenceType(Type.getTypeName(type), type, cardinality, ""));
		}

		if (error) 
			return null;
		
		return fTypes;
	}
	
	public boolean isMerged() {
		if (methods.size() == 0)
			throw new RuntimeException("No method(s).");
			
		if (_arity == 0 && methods.size() > 1)
			throw new RuntimeException("Arity zero & "+methods.size()+" methods.");
		
		return true;
		//method.isAnnotationPresent(Deprecated.class)

	}

	public boolean isDeprecated() {
		for (Method method : methods) {
			if (!method.isAnnotationPresent(Deprecated.class)) {
				return false;
			}
		}
		return true;
	}
}