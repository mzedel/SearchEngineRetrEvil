package de.hpi.krestel.mySearchEngine;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * The handler for parsing XML data via SAX.
 * Extracts id, title and text of every page which is a Wikipedia article
 * and not a redirect.
 * Calls the given Indexer for every page and at the end of the XML dump.
 * 
 * StringBuilders are used to concat parts of the titles and texts, as it 
 * seems that long texts may cause "characters()" to be called several 
 * times. StringBuilder is an alternative of StringBuffer for single threads
 * (faster, but not synchronized) which should work because the parser works
 * sequentially in one thread.
 */
class SAXHandler extends DefaultHandler {

	private final SearchEngineRetrEvil searchEngineRetrEvil;

	private IndexHandler indexer;	// builds the index
	
	private Long id;				// id of current page
	private StringBuilder title;	// title of current page
	private StringBuilder text;		// text of current page
	
	boolean inPage, 				// parsing <page>
		inTitle, 					// parsing <title>
		inNs, 						// parsing <ns>
		isArticle, 					// whether the page is an article
		inId, 						// parsing <id>
		isRedirect, 				// whether the page is a redirection
		inText = false;				// parsing <text>
	
	public SAXHandler(SearchEngineRetrEvil searchEngineRetrEvil, IndexHandler indexer) {
		this.searchEngineRetrEvil = searchEngineRetrEvil;
		this.indexer = indexer;
	}
	
	private void startNewPage() {
		// reset booleans, initialize new id and buffers
		this.inPage = true;
		this.inTitle = false;
		this.inNs = false;
		this.isArticle = false;
		this.inId = false;
		this.isRedirect = false;
		this.inText = false;
		
		this.id = null;
		this.title = new StringBuilder(100);
		this.text = new StringBuilder(1000);
	}
	
	private void endPage() {
		// reset boolean
		this.inPage = false;
		/*
		 * Policy: index the page if and only if all necessary data can be 
		 * provided and the page is relevant (i.e., an article and not an 
		 * indirection). If not all necessary information for a relevant
		 * page can be provided, log a message.
		 */
		if (this.isArticle && !this.isRedirect) {
			if (this.id != null && this.title.length() > 0 && this.text.length() > 0) {
				this.indexer.indexPage(this.id, this.title.toString(), this.text.toString());
			} else {
				this.searchEngineRetrEvil.log("");
			}
		}
	}

	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		if (qName.equalsIgnoreCase("page")) {
			// start parsing a new page, reset attributes
			this.startNewPage();
		} else if (this.inPage && qName.equalsIgnoreCase("title")) {
			this.inTitle = true;
		} else if (this.inPage && qName.equalsIgnoreCase("ns")) {
			this.inNs = true;
		} else if (this.inPage && qName.equalsIgnoreCase("id")) {
			this.inId = true;
		} else if (this.inPage && qName.equalsIgnoreCase("redirect")) {
			/*
			 * Policy: suppose that every page which has a tag <redirect />
			 * (regardless of the "title" attribute of that tag) is a
			 * redirection
			 */
			this.isRedirect = true;
		} else if (this.inPage && qName.equalsIgnoreCase("text")) {
			this.inText = true;
		}
	}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		if (qName.equalsIgnoreCase("page")) {
			// finish parsing the current page, call the indexer
			this.endPage();
		} else if (this.inTitle && qName.equalsIgnoreCase("title")) {
			this.inTitle = false;
		} else if (this.inNs && qName.equalsIgnoreCase("ns")) {
			this.inNs = false;
		} else if (this.inId && qName.equalsIgnoreCase("id")) {
			this.inId = false;
		} else if (this.inText && qName.equalsIgnoreCase("text")) {
			this.inText = false;
		}
	}
	
	@Override
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		if (this.inTitle) {
			// get the title of the page
			this.title.append(new String(ch, start, length));
		} else if (this.inNs) {
			if (new String(ch, start, length).equals("0")) {
				/* 
				 * page is an article (i.e., it is relevant for the index 
				 * unless it is a redirection)
				 */
				this.isArticle = true;
			}	// else: page is not an article and, therefore, irrelevant
		} else if (this.inId && this.id == null) {
			// try to get the page id (subsequent ids are from revisions) as a Long
			try {
				this.id = Long.parseLong(new String(ch, start, length));
			} catch (NumberFormatException e) {
				// continue, but log exception
				this.searchEngineRetrEvil.log("SAXParser: could not parse ID of the current page: "
						+ e.getMessage());
			}
		} else if (this.isArticle && !this.isRedirect && this.inText) {
			// get the text of the page (may be done in several steps)
			this.text.append(new String(ch, start, length));
		}
	}

	@Override
	public void endDocument() throws SAXException {
		// parsing is finished, Indexer may now merge the index
		this.indexer.createIndex();
	}
	
}