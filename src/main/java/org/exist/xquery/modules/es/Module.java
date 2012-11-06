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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.exist.xquery.AbstractInternalModule;
import org.exist.xquery.Function;
import org.exist.xquery.FunctionDef;
import org.exist.xquery.FunctionSignature;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 * 
 */
public class Module extends AbstractInternalModule {

    public final static String NAMESPACE_URI = "http://exist-db.org/xquery/elasticsearch";

    public final static String PREFIX = "es";
    public final static String INCLUSION_DATE = "2012-10-26";
    public final static String RELEASED_IN_VERSION = "eXist-2.1";

    private final static List<FunctionDef> functions = new ArrayList<FunctionDef>();
	
    private static void addFunction(Class<? extends Function> clazz) {
    	
    	try {
    		FunctionSignature[] signs = (FunctionSignature[])clazz.getField("signatures").get(clazz);
    		if (signs == null)
    			return;
    		
    		for (FunctionSignature sign : signs) {
    			functions.add(new FunctionDef(sign, clazz));
    		}
		} catch (Exception e) {
		}
    	
    }
    static {
    	addFunction(SearchFunction.class);
    	functions.addAll(Binding.defs);
    };
    
    static {
    	;
    }

    public Module(Map<String, List<? extends Object>> parameters) {
        super(functions.toArray(new FunctionDef[functions.size()]), parameters);
    }

    @Override
    public String getNamespaceURI() {
            return NAMESPACE_URI;
    }

    @Override
    public String getDefaultPrefix() {
            return PREFIX;
    }

    @Override
    public String getDescription() {
            return "A module for ...";
    }

    @Override
    public String getReleaseVersion() {
        return RELEASED_IN_VERSION;
    }
    
    public static void main(String[] args) {
		System.out.println(Arrays.toString(functions.toArray()));
	}
}
