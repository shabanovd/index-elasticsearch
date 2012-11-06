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
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.FacetBuilders;
import org.exist.dom.QName;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class Binding extends AFunction {
	
	protected static List<FunctionDef> defs = new ArrayList<FunctionDef>();
	
	private static Map<Class<?>, Short> analized = new HashMap<Class<?>, Short>();
	
	static {
		analize(QueryBuilders.class);
		analize(FacetBuilders.class);
	}
	
	private static short analize(Class<?> clazz) {
		
		Short flag = analized.get(clazz);
		if (flag != null)
			return flag;
		
		System.out.println("==============================");
		System.out.println(clazz);
		
		Short thisType = 0;

		Map<String, BindingMethodsAnalizer> groups = new HashMap<String, BindingMethodsAnalizer>();
		Method[] methods = clazz.getDeclaredMethods();
		for (Method method : methods) {
			String name = readable(method.getName());
			
			int arity = method.getParameterTypes().length;

			String key = name+":"+String.valueOf(arity);
			
			BindingMethodsAnalizer analizer = groups.get(key);
			if (analizer == null) {
				analizer = new BindingMethodsAnalizer(clazz, name, arity);
				groups.put(key, analizer);
			}
			analizer.add(method);

			if (thisType < 1 && Modifier.isStatic(method.getModifiers()))
				thisType = 1;
			else if (thisType < 2)
				thisType = 2;
		}
		analized.put(clazz, thisType);
			
		for (BindingMethodsAnalizer method : groups.values()) {
			String name = method.getName();

			System.out.println(name+" - "+method);
			
			if (!method.isMerged())
				continue;

			List<SequenceType> fTypes = method.functionParameter();
			if (fTypes == null)
				continue;
			
			Short subT = analize(method.getReturnType());
			if (subT == 1) {
				fTypes.add(new FunctionParameterSequenceType("details", Type.FUNCTION_REFERENCE, Cardinality.ONE_OR_MORE, ""));
			} else if (subT == 2) {
				defs.add( 
					new FunctionDef(
						new FSBinding(
							clazz, method,
							new QName(name, Module.NAMESPACE_URI, Module.PREFIX),
							fTypes.toArray(new SequenceType[fTypes.size()]),
							new FunctionReturnSequenceType(Type.FUNCTION_REFERENCE, Cardinality.EXACTLY_ONE, ""),
							method.isDeprecated()
						),
						Binding.class
					) 
				);

				fTypes.add(new FunctionParameterSequenceType("details", Type.FUNCTION_REFERENCE, Cardinality.ONE_OR_MORE, ""));
			}
			
			defs.add( 
				new FunctionDef(
					new FSBinding(
						clazz, method,
						new QName(name, Module.NAMESPACE_URI, Module.PREFIX),
						fTypes.toArray(new SequenceType[fTypes.size()]),
						new FunctionReturnSequenceType(Type.FUNCTION_REFERENCE, Cardinality.EXACTLY_ONE, ""),
						method.isDeprecated()
					),
					Binding.class
				) 
			);
		}
		return thisType;
	}
	
	private static String readable(String name) {
		String lowName = name.toLowerCase();

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < name.length(); i++) {
			if (name.charAt(i) != lowName.charAt(i)) {
				sb.append("-");
			}
			sb.append(lowName.charAt(i));
		}
		return sb.toString();
	}
	
	public Binding(XQueryContext context, FunctionSignature signature) {
		super(context, signature);
	}

	@Override
	public ToXContent toXContent(Sequence contextSequence) throws XPathException {
		BindingMethodsAnalizer binding = ((FSBinding)mySignature)._method;
		
		ToXContent toXContent = _toXContent(binding, contextSequence);
		
		if (getArgumentCount() > binding._arity) {
			for (int i = binding._arity; i < getArgumentCount(); i++) {
				Sequence fCalls = eval(i, contextSequence);
				for (int j = 0; j < fCalls.getItemCount(); j++) {
					Item fCall = fCalls.itemAt(j);
					if (fCall instanceof Builder) {
						toXContent = ((Builder) fCall).proccessXContent(toXContent, contextSequence);
					} else {
						throw new XPathException(this, mySignature.toString());
					}
				}
			}
			
		}
		return toXContent;
	}
	
	public ToXContent _toXContent(BindingMethodsAnalizer binding, Sequence contextSequence) throws XPathException {
		if (binding._arity == 0) {
			return invokeMethod(binding._owner, (Method)binding.methods.get(0));
		
		} else if (binding._arity >= 1) {
			Iterator<Method> methodsIterator = binding.methods.iterator();
			Method method = nextMethod(methodsIterator);
			  
			Object[] objs = new Object[binding._arity];
			for (int i = 0; i < binding._arity; i++) {
				Sequence param = eval(i, contextSequence);
				
				Class<?> clazz = binding.same[i];
				if (clazz != null) {
					if (clazz == String.class) {
						objs[i] = param.getStringValue();
					} else if (clazz == org.elasticsearch.index.query.QueryBuilder.class) {
				    	if (param instanceof Builder) {
				    		objs[i] = ((Builder) param).toXContent(contextSequence);
						} else {
							throw new XPathException(this, mySignature.toString());
						}
					} else {
						throw new XPathException(this, mySignature.toString());
					}
				} else {
					switch (param.getItemType()) {
					case Type.STRING:
						while (method != null) {
							if (method.getParameterTypes()[i] == String.class) {
								objs[i] = param.getStringValue();
								break;
							}
							
							method = nextMethod(methodsIterator);
						}
						
						break;

					default:
						throw new XPathException(this, mySignature.toString());
					} 
				}
			}
    		return invokeMethod(binding._owner, method, objs);
			
		}
		throw new XPathException(this, mySignature.toString());
	}
	
	private Method nextMethod(Iterator<Method> methodsIterator) throws XPathException {
		if (!methodsIterator.hasNext())
			throw new XPathException(this, mySignature.toString());
		return methodsIterator.next();
	}
	
	private ToXContent invokeMethod(Object obj, Method method, Object... args) throws XPathException {
		try {
			return (ToXContent) method.invoke(obj, args);
		} catch (Exception e) {
			if (e instanceof XPathException) {
				throw (XPathException) e;
			}
			throw new XPathException(this, e);
		}
	}
	
	private Object[] sss(BindingMethodsAnalizer binding, Sequence contextSequence) throws XPathException {
		Iterator<Method> methodsIterator = binding.methods.iterator();
		Method method = nextMethod(methodsIterator);
		  
		Object[] objs = new Object[binding._arity];
		for (int i = 0; i < binding._arity; i++) {
			Sequence param = eval(i, contextSequence);
			
			Class<?> clazz = binding.same[i];
			if (clazz != null) {
				if (clazz == String.class) {
					objs[i] = param.getStringValue();
				} else if (clazz == org.elasticsearch.index.query.QueryBuilder.class) {
			    	if (param instanceof Builder) {
			    		objs[i] = ((Builder) param).toXContent(contextSequence);
					} else {
						throw new XPathException(this, mySignature.toString());
					}
				} else {
					throw new XPathException(this, mySignature.toString());
				}
			} else {
				switch (param.getItemType()) {
				case Type.STRING:
					while (method != null) {
						if (method.getParameterTypes()[i] == String.class) {
							objs[i] = param.getStringValue();
							break;
						}
						method = nextMethod(methodsIterator);
					}
					
					break;

				default:
					throw new XPathException(this, mySignature.toString());
				} 
			}
		}
		return objs;
	}

	@Override
	public ToXContent proccessXContent(ToXContent toXContent, Sequence contextSequence) throws XPathException {
		System.out.println(mySignature);
		throw new XPathException(this, mySignature.toString());
	}
	
	public static void main(String[] args) {
		System.out.println("");
	}
}
