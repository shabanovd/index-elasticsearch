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

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.facet.AbstractFacetBuilder;
import org.exist.dom.DocumentSet;
import org.exist.dom.NodeSet;
import org.exist.dom.QName;
import org.exist.index.es.*;
import org.exist.memtree.DocumentBuilderReceiver;
import org.exist.memtree.MemTreeBuilder;
import org.exist.memtree.NodeImpl;
import org.exist.util.serializer.AttrList;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 * 
 */
public class SearchFunction extends BasicFunction {
	
	private final static QName SEARCH_RESPONSE = new QName("search-response", Module.NAMESPACE_URI, Module.PREFIX);

    public final static FunctionSignature signatures[] = { 
		new FunctionSignature(
				new QName("search", Module.NAMESPACE_URI, Module.PREFIX),
				"...",
				new SequenceType[] { 
                    new FunctionParameterSequenceType("node-set", Type.NODE, Cardinality.ZERO_OR_MORE, "The node set"),
					new FunctionParameterSequenceType("params", Type.FUNCTION_REFERENCE, Cardinality.ONE_OR_MORE, ""), 
					new FunctionParameterSequenceType("from", Type.INT, Cardinality.ONE, ""),
					new FunctionParameterSequenceType("size", Type.INT, Cardinality.ONE, "") 
				}, 
	            new FunctionReturnSequenceType(Type.NODE, Cardinality.ZERO_OR_MORE, "")
			) 
	};

	public SearchFunction(XQueryContext context, FunctionSignature signature) {
		super(context, signature);
	}

	public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        NodeSet nodes = args[0].toNodeSet();
        DocumentSet docs = nodes.getDocumentSet();

		ESIndexWorker index = (ESIndexWorker) context.getBroker().getIndexController().getWorkerByIndexId(ElasticsearchIndex.getID());
		if (index == null)
			throw new XPathException(this, "Elasticsearch does not active.");

		Client client = index.getNode().client();
		
		SearchRequestBuilder builder = client.prepareSearch();
		
		Sequence params = args[1];
		for (int i = 0; i < params.getItemCount(); i++) {
			Item param = params.itemAt(i);
//			System.out.println(param.getClass());
			
			if (param instanceof Builder) {
				ToXContent b = ((Builder)param).toXContent(contextSequence);
				if (b instanceof QueryBuilder) {
					builder = builder.setQuery((QueryBuilder) b);
					
				} else if (b instanceof AbstractFacetBuilder) {
						builder = builder.addFacet((AbstractFacetBuilder) b);
				} else
					throw new XPathException(this, ErrorCodes.ERROR, "wrong param "+param, (Sequence)param);
				
			} else
				throw new XPathException(this, ErrorCodes.ERROR, "wrong param "+param, (Sequence)param);
		}
    	
		builder = builder.setFrom(Utils.getInt(args[2].itemAt(0)));
		builder = builder.setSize(Utils.getInt(args[3].itemAt(0)));

		SearchResponse res = builder.execute().actionGet();
		
		System.out.println(res.toString());
		
//        context.pushDocumentContext();
//
//        MemTreeBuilder mBuilder = context.getDocumentBuilder(true);
//        DocumentBuilderReceiver receiver = new DocumentBuilderReceiver(mBuilder);
//
        try {
//        	AttrList attrs = new AttrList();
//	        receiver.startElement(SEARCH_RESPONSE, attrs);
	        
			Matcher matcher = new Matcher(getContext().getDatabase(), docs);
		        		
			res.toXContent(
				new XContentBuilder(new XmlXContent(matcher), null),
				null
			);
			
			return matcher.nodes;

//	        System.out.println(res.took());
//	        Facets facets = res.getFacets();
//	        if (facets == null) {
//	        	System.out.println("no facets");
//	        } else {
//		        for (Entry<String, Facet> entry : facets.facetsAsMap().entrySet()) {
//		        	System.out.println(entry.getKey()+" "+entry.getValue());
//		        	
//		        	InternalCountHistogramFacet ff = (InternalCountHistogramFacet) entry.getValue();
//		        	for (CountEntry e : ff.entries()) {
//		        		System.out.println(e.getKey()+" "+e.count());
//		        	}
//		        }
//	        }
//	        System.out.println(res.hits().totalHits());
//			for (SearchHit hit : res.getHits()) {
//				System.out.println(hit.sourceAsString());
//			}
	
//	        receiver.endElement(SEARCH_RESPONSE);
        } catch (Exception e) {
        	throw new XPathException(this, e);
		}
//
//        context.popDocumentContext();
//        NodeImpl node =  mBuilder.getDocument();
//    	return node;
	}
}