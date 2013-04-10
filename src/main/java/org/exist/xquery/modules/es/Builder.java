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

import java.util.List;

import org.elasticsearch.common.xcontent.ToXContent;
import org.exist.xquery.Expression;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionReference;
import org.exist.xquery.value.Sequence;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class Builder extends FunctionReference {
	
	AFunction function;
	
	public Builder(XQueryContext context, AFunction function, List<Expression> arguments) {
		super(new Call(context, function.getSignature().getName(), arguments));
		
		this.function = function;
	}
	
	public ToXContent toXContent(Sequence contextSequence) throws XPathException {
		return function.toXContent(contextSequence);
	}

	public ToXContent proccessXContent(ToXContent toXContent, Sequence contextSequence) throws XPathException {
		return function.proccessXContent(toXContent, contextSequence);
	}
}
