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

import org.elasticsearch.common.xcontent.ToXContent;
import org.exist.xquery.Function;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.Sequence;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public abstract class AFunction extends Function {

	public AFunction(XQueryContext context, FunctionSignature signature) {
		super(context, signature);
	}
	
    public Sequence eval(Sequence contextSequence, Item contextItem) throws XPathException {
		return new Builder(context, this, steps);
	}

    protected Sequence eval(int pos, Sequence contextSequence) throws XPathException {
        return getArgument(pos).eval(contextSequence, null);
    }

    public abstract ToXContent toXContent(Sequence contextSequence) throws XPathException;

	public abstract ToXContent proccessXContent(ToXContent toXContent, Sequence contextSequence) throws XPathException;
}
