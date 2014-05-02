package de.hpi.krestel.mySearchEngine;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
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
 * - Apache Lucene GermanAnalyzer
 *   is used for tokenizing, stemming, stopword removal (uses the default 
 *   Snowball stop word list)
 */

public class SearchEngineRetrEvil extends SearchEngine {
	
	public SearchEngineRetrEvil() {
		// This should stay as is! Don't add anything here!
		super();	
	}

	/**
	 * An Index holds a map which maps terms to TermLists (which hold the
	 * occurrences of that term in documents).
	 * Index uses {@link TreeMap}, whose keys are ordered, to provide the
	 * ordering of terms.
	 * This is a utility class. It does not check for null values.
	 * 
	 * TODO: implement merging of Index / TermList parts
	 */
	private class Index {
		
		/**
		 * A TermList is a inverted list, i.e., a list of documents and
		 * positions in these documents where a particular term occurs.
		 * Each document and position is represented by ids which are Longs.
		 * Documents as well as positions within documents are ordered,
		 * such as (conceptually):
		 * 		"apfel": 1: 3, 36, 47; 3: 2, 28, 91, 106
		 * TermList uses {@link TreeMap}, whose keys are ordered, as well as 
		 * {@link TreeSet}, whose elements are ordered according to their 
		 * natural ordering.
		 * This is a utility class. It does not check for null values.
		 * 
		 * TODO: implement delta encoding for document ids and positions
		 */
		public class TermList {
			
			/**
			 * The map of occurrences for this list's term.
			 * We use Collection instead of List because we want to use
			 * TreeSets, which do not implement the interface List. Iterate
			 * over the elements via {@link Collection#iterator()}.
			 */
			private Map<Long, Collection<Long>> occurrences;
			
			/**
			 * Create a new TermList.
			 * Initialize the map.
			 */
			public TermList() {
				this.occurrences = new TreeMap<Long, Collection<Long>>();
			}
			
			/**
			 * Add an occurrence (i.e., the id of the document in which the
			 * term occurs and the position in that document) to this TermList.
			 * If the document is not already present in the map, a new List
			 * of positions is created for that document and put in the map.
			 * Otherwise, the position is just added to the existing list.
			 * If the position is already known, nothing will be changed.
			 * @param documentId the id of the document
			 * @param position the position in the document
			 */
			public void addOccurrence(Long documentId, Long position) {
				if (!this.occurrences.containsKey(documentId)) {
					this.createCollectionForDocument(documentId);
				}
				Collection<Long> positions = this.occurrences.get(documentId);
				/*
				 * the following test is actually not necessary with TreeSet, 
				 * but performed anyway as we only use the interface List here
				 */
				if (!positions.contains(position)) {
					positions.add(position);
				}
			}
			
			/**
			 * Create an empty Collection and add it to the Map of occurrences,
			 * using the id as key.
			 * If there is already a Collection, nothing will be changed.
			 * @param documentId the id of the document
			 */
			private void createCollectionForDocument(Long documentId) {
				if (!this.occurrences.containsKey(documentId)) {
					this.occurrences.put(documentId, new TreeSet<Long>());
				}
			}
			
			/**
			 * Provide a String representation for this TermList which is
			 * suited for human-readable output. For indexing, use
			 * {@link #toIndexString()}.
			 * @return a String representation for nice output
			 */
			public String toString() {
				StringBuilder result = new StringBuilder();
				
				result.append("( ");
				for (Long documentId : this.occurrences.keySet()) {	// uses iterator
					result.append(documentId + ": [ ");
					Collection<Long> positions = this.occurrences.get(documentId);
					if (positions != null) {
						for (Long position : positions) {	// uses iterator
							result.append(position + " ");
						}
					}
					result.append("] ");
				}
				result.append(")");
				
				return result.toString();
			}
			
			/**
			 * Provide a String representation for this TermList which is
			 * suited for indexing.
			 * Example:
			 * 	1:1,2,3;6:1,3,8.
			 * @return a String representation for indexing
			 */
			public String toIndexString() {
				StringBuilder result = new StringBuilder();
				
				for (Long documentId : this.occurrences.keySet()) {	// uses iterator
					result.append(documentId + ":");
					Collection<Long> positions = this.occurrences.get(documentId);
					if (positions != null) {
						for (Long position : positions) {	// uses iterator
							result.append(position + ",");
						}
					}
					// remove last ',' with ';'
					result
						.deleteCharAt(result.lastIndexOf(","))
						.append(";");
				}
				// remove last ';' with '.'
				result
					.deleteCharAt(result.lastIndexOf(";"))
					.append(".");
				
				return result.toString();
			}
			
		}
		
		private Map<String, TermList> termLists;
		
		/**
		 * Create a new Index.
		 * Initialize the map.
		 */
		public Index() {
			this.termLists = new TreeMap<String, TermList>();
		}
		
		/**
		 * Adds the occurrence of a term in a particular document at a particular
		 * position to the index. Creates a new TermList for that term, if necessary.
		 * If the position is already known, nothing will be changed.
		 * @param term the term
		 * @param documentId the id of the document
		 * @param position the position within the document
		 */
		public void addTermOccurrence(String term, Long documentId, Long position) {
			if (!this.termLists.containsKey(term)) {
				this.createListForTerm(term);
			}
			// delegate the rest to the TermList
			this.termLists.get(term).addOccurrence(documentId, position);;
		}
		
		/**
		 * Creates a new TermList for the given term and adds it to the map,
		 * using the term as key.
		 * If the term is already present in the map, nothing will be changed.
		 * @param term the term to create a list for
		 */
		private void createListForTerm(String term) {
			if (!this.termLists.containsKey(term)) {
				this.termLists.put(term, new TermList());
			}
		}
		
		/**
		 * Provide a String representation for this Index which is
		 * suited for human-readable output.
		 * @return a String representation for nice output
		 */
		public String toString() {
			StringBuilder result = new StringBuilder();
			
			result.append("{\n");
			for (String term : this.termLists.keySet()) {	// uses iterator
				result.append(term + ": ");
				TermList list = this.termLists.get(term);
				if (list != null) {
					result.append(list + "\n");	// uses toString()
				}
			}
			result.append("}");
			
			return result.toString();
		}
		
		/**
		 * Getter for {@link #termLists}. Should be used for writing the index
		 * (via {@link TermList#toIndexString()}).
		 * @return the map of lists of this index
		 */
		public Map<String, TermList> getTermLists() {
			return this.termLists;
		}
		
	}
	
	/**
	 * Handles everything related to the index. Deals with related I/O.
	 * Gets id, title and text per article from the SAXHandler.
	 * Merges everything into the final index file once the SAXHandler has
	 * finished parsing.
	 * (in the future:) Provides information for the query engine.
	 * 
	 * TODO: implement writing / merging of Index parts to avoid memory problems
	 */
	private class Indexer {
		
		// name of the file which stores the index
		private static final String indexFileName = "index.txt";
		// name of the file which stores the seeklist
		private static final String seekListFileName = "seeklist.txt";
		// name of the file which stores the mapping of document ids and titles
		private static final String titlesFileName = "titles.txt";
		
		// directory of files to be read / written
		private String dir;
		
		// the analyzer for pre-processing of documents and queries
		private Analyzer analyzer;
		
		// the index
		private Index index;
		// the seeklist (term - offset)
		private Map<String, Long> seeklist;
		// the mapping from document ids to titles
		private Map<Long, String> titles;
		
		/**
		 * Create an Indexer which handles index files in the given directory.
		 * This includes the creation of an analyzer for pre-processing
		 * as well as the initialization of all index structures.
		 * Uses {@link TreeMap} for maps whose keys are ordered.
		 * @param dir the directory for all index files
		 */
		public Indexer(String dir) {
			this.dir = dir;
			this.analyzer = this.createAnalyzer();
			this.index = new Index();
			this.seeklist = new TreeMap<String, Long>();
			this.titles = new TreeMap<Long, String>();
		}
		
		/**
		 * Create and configure the Analyser to be used.
		 * @return an analyzer for text processing
		 */
		private Analyzer createAnalyzer() {
			/* 
			 * Specify the compatibility version. Influences the behaviour of
			 * components. See 
			 * http://lucene.apache.org/core/4_7_2/analyzers-common/org/apache/
			 * 	lucene/analysis/de/GermanAnalyzer.html#GermanAnalyzer
			 * 	%28org.apache.lucene.util.Version%29
			 */
			Version version = Version.LUCENE_47;	// newest version
			
			/* 
			 * Create and return the analyzer with the given compatibility version.
			 * As of version 3.1, Snowball stopwords are used per default (can
			 * be accessed via GermanAnalyzer.getDefaultStopSet());
			 * 
			 * TODO: perhaps specify another stop word list; the default list
			 * seems to include "daß" instead of "dass"
			 * 
			 * TODO: there may be a problem with some German characters ('ü' etc.),
			 * so we might replace these characters (or all UTF-8 characters which
			 * are not ASCII in general) with some other representation.
			 */
			Analyzer analyzer = new GermanAnalyzer(version);
			
			return analyzer;
		}
		
		/**
		 * Pre-process raw text, i.e., tokenize, stem, use stopword list.
		 * Return a list of terms. See
		 * https://lucene.apache.org/core/4_7_2/core/org/apache/
		 * 	lucene/analysis/package-summary.html
		 * @param text the text to be processed
		 */
		private List<String> processRawText(String text) throws IOException {
			/* 
			 * Create the event stream; subsequent calls to analyzer.tokenStream
			 * will return the same stream; "fieldName" should not be relevant for
			 * our case (refers to segments of documents in normal applications 
			 * of the Lucene framework).
			 * There are several attributes which can be accessed during streaming.
			 * CharTermAttribute refers to the string values of terms in the text
			 * which is the only thing that is of interest for us.
			 */
			TokenStream stream = this.analyzer.tokenStream("fieldName", text);
			CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
			
			// initiate the list with an initial capacity of 1000
			List<String> terms = new ArrayList<String>(1000);
			
			try {
				stream.reset();	// necessary for subsequent calls to analyzer.tokenStream
				while (stream.incrementToken()) {	// proceed to the next term
					terms.add(termAtt.toString());	// get the term
				}
				stream.end();
			} finally {
				stream.close();
			}
			
			return terms;
		}
		
		/**
		 * Add the id-title-mapping to the respective map.
		 * Add the occurrences of all terms in the given text to the index.
		 * If an IOException occurs, log it, but proceed.
		 * @param id the id of the document
		 * @param title the title of the document
		 * @param text the text of the document
		 */
		public void indexPage(Long id, String title, String text) {
			// note id - title - mapping
			if (!this.titles.containsKey(id)) {
				this.titles.put(id, title);
			}
			
			try {
				// process text (tokenizing, stopping, stemming)
				List<String> terms = this.processRawText(text);
				// add occurrences to index
				for (int position = 0; position < terms.size(); position++) {
					String term = terms.get(position);
					this.index.addTermOccurrence(term, id, (long) position);
				}
			} catch (IOException e) {
				// an IOException was thrown by the Analyzer
				log("Could not index page " + id + ": " + e.getMessage());
			}
		}
		
		/**
		 * Merges all parts of the index.
		 * TODO: implement merging
		 * Creates the seeklist.
		 * Writes the index, the seeklist and the id-titles-mapping to 
		 * files (one file each).
		 * If an IOException occurs, log it, but proceed.
		 */
		public void createIndex() {
			try {
				// get the index file; if it does already exist, delete it
				File indexFile = this.getErasedFile(this.dir + Indexer.indexFileName);
				/* 
				 * get random access file for index with read / write, attempts 
				 * to make nonexistent file
				 */
				RandomAccessFile raIndexFile = new RandomAccessFile(indexFile, "rw");
				// get map of terms and their occurrence lists
				Map<String, Index.TermList> termLists = this.index.getTermLists();
				// write each occurrence list to the file and note the offsets
				for (String term : termLists.keySet()) {	// uses iterator
					// note the offset
					long offset = raIndexFile.getFilePointer();
					this.seeklist.put(term, offset);
					// write the list using custom toIndexString method of TermList
					raIndexFile.writeChars(termLists.get(term).toIndexString());
				}
				// close the index file
				raIndexFile.close();
				
				/*
				 * write the seeklist to a file; it is not necessary to use 
				 * RandomAccessFile here, but we just use the same pattern
				 */
				File seekListFile = this.getErasedFile(this.dir + Indexer.seekListFileName);
				RandomAccessFile raSeekListFile = new RandomAccessFile(seekListFile, "rw");
				raSeekListFile.writeChars(this.seekListToString());
				raSeekListFile.close();
				
				/*
				 * write the id-title-mapping to a file
				 */
				File titlesFile = this.getErasedFile(this.dir + Indexer.titlesFileName);
				RandomAccessFile raTitlesFile = new RandomAccessFile(titlesFile, "rw");
				raTitlesFile.writeChars(this.titlesToString());
				raTitlesFile.close();
			} catch (IOException e) {
				log("Cannot create index files: " + e.getMessage());
			}
		}
		
		/**
		 * Get a {@link File} for the given path. If it does already exist,
		 * try to erase it. If deleting it does not work, throw an 
		 * {@link IOException}.
		 * @param filePath the path to the file
		 * @throws IOException
		 */
		private File getErasedFile(String filePath) throws IOException {
			File file = new File(filePath);
			if (file.exists()) {
				// try to delete
				if (!file.delete()) {
					throw new IOException("Could not delete the existent index file");
				}
			}
			return file;
		}
		
		/** 
		 * Stringifies the seek list for writing it to a file. Uses the pattern:
		 * 	apfel\t0\tbaum\t1608.	(\t are actual tab characters)
		 * TODO: make seeklist more space efficient
		 * @return a string representation of the seek list
		 */
		private String seekListToString() {
			StringBuilder result = new StringBuilder();
			
			for (String term : this.seeklist.keySet()) {	// uses iterator
				result
					.append(term)
					.append('\t')
					.append(this.seeklist.get(term))
					.append('\t');
			}
			// remove the last '\t'
			result.deleteCharAt(result.lastIndexOf("\t"));
			
			return result.toString();
		}
		
		/**
		 * Stringifies the id-title-mapping for writing it to a file. Uses the pattern:
		 * 	1\tAlan Smithee\t3\tActinium	(\t are actual tab characters)
		 * TODO: make this more space efficient
		 * @return a string representation of the id-title-mapping
		 */
		private String titlesToString() {
			StringBuilder result = new StringBuilder();
			
			for (Long id : this.titles.keySet()) {	// uses iterator
				result
					.append(id)
					.append('\t')
					.append(this.titles.get(id))
					.append('\t');
			}
			// remove the last '\t'
			result.deleteCharAt(result.lastIndexOf("\t"));
			
			return result.toString();
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
			this.indexer.createIndex();
		}
		
	}
	
	/*
	 * Parse the dump and create the index in the given "dir".
	 * The dump may be located elsewhere.
	 */
	@Override
	void index(String dir) {
		if (dir == null) {
			this.log("abort: dir is null");
			return;
		}
		
		// ensure that the dir path ends with a '/'
		if (!dir.endsWith("/")) {
			dir = dir.concat("/");
		}
		
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
