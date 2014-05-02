package de.hpi.krestel.mySearchEngine;

import java.io.File;
import java.util.ArrayList;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/* This is your file! implement your search engine here!
 * 
 * Describe your search engine briefly:
 *  - multi-threaded?
 *  - stemming?
 *  - stopword removal?
 *  - index algorithm?
 *  - etc.  
 * 
 */

public class SearchEngineRetrEvil extends SearchEngine {
	
	public SearchEngineRetrEvil() {
		// This should stay as is! Don't add anything here!
		super();	
	}

	/*
	 * Handles everything related to the creation of the index.
	 * Gets id, title and text per article from the SAXHandler.
	 * Merges everything into the final index file once the SAXHandler has
	 * finished parsing.
	 */
	private class Indexer {
		
		private String dir;
		
		public Indexer(String dir) {
			this.dir = dir;
		}
		
		public void indexPage(Long id, String title, String text) {
			// TODO: implement
		}
		
		public void mergeIndices() {
			// TODO: implement
		}
		
	}
	
	/*
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
	private class SAXHandler extends DefaultHandler {

		private Indexer indexer;		// builds the index
		
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
		
		public SAXHandler(Indexer indexer) {
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
					log("");
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
			} else if (this.inPage && qName.equalsIgnoreCase("ns")) {
				this.inNs = false;
			} else if (this.inTitle && qName.equalsIgnoreCase("id")) {
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
			} else if (this.inId) {
				// try to get the id as a Long
				try {
					this.id = Long.parseLong(new String(ch, start, length));
				} catch (NumberFormatException e) {
					// continue, but log exception
					log("SAXParser: could not parse ID of the current page: "
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
			this.indexer.mergeIndices();
		}
		
	}
	
	/*
	 * Parse the dump and create the index in the given "dir".
	 * The dump may be located elsewhere.
	 */
	@Override
	void index(String dir) {
		this.log("abort: dir is null");
		
		// get dump file TODO: make that more general
		String dumpFile = new File(dir).getParent() + "/" + "testDump.xml";

		// create the indexer with the target dir
		Indexer indexer = new Indexer(dir);
		
		try {
			// get the SAX parser with the appropriate handler
			SAXParser saxParser = SAXParserFactory.newInstance().newSAXParser();
			DefaultHandler saxHandler = new SAXHandler(indexer);
			
			// parse the dump
			saxParser.parse(dumpFile, saxHandler);
		} catch (Exception e) {
			this.log("Exception during SAX parsing: " + e.toString());
		}
	}

	@Override
	boolean loadIndex(String directory) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	ArrayList<String> search(String query, int topK, int prf) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	Double computeNdcg(String query, ArrayList<String> ranking, int ndcgAt) {
		// TODO Auto-generated method stub
		return null;
	}
}
