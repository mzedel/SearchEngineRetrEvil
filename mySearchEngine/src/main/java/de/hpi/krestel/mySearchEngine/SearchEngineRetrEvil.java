package de.hpi.krestel.mySearchEngine;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.de.GermanStemFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/*
 * Describe your search engine briefly:
 *  - multi-threaded?
 *  - stemming?
 *  - stopword removal?
 *  - index algorithm?
 *  - etc.
 */

public class SearchEngineRetrEvil extends SearchEngine {
	
	/**
	 * Boolean operator "AND" in upper case
	 */
	private static final String AND = "AND";
	/**
	 * Boolean operator "OR" in upper case
	 */
	private static final String OR = "OR";
	/**
	 * Boolean operator "BUT NOT" in upper case
	 */
	private static final String BUT_NOT = "BUT NOT";
	/**
	 * List of all boolean operators (for convenience)
	 */
	private static final String[] BOOLEAN_OPERATORS = new String[] { 
		AND, OR, BUT_NOT 
	};
	
	/**
	 * BM25 parameter <tt>k1</tt> which regulates the weighting of the term
	 * frequency in a document (<tt>0</tt>: ignored; <tt>1.2</tt>: usual).
	 */
	private static final double BM25_K1 = 1.2;
	/**
	 * BM25 parameter <tt>k2</tt> which regulates the weighting of the term
	 * frequency in the query (<tt>0</tt>: ignored; <tt>0<=k2<=1000</tt>: usual).
	 */
	private static final double BM25_K2 = 100.0;
	/**
	 * BM25 parameter <tt>b</tt> which regulates the document length normalization
	 * regarding the weighting of the term frequency in a document (<tt>0</tt>: 
	 * no normalization; <tt>1</tt>: full normalization).<br>
	 * If <tt>b</tt> is <tt>0</tt>,
	 * K can be computed without information about document length and average
	 * document length.
	 */
	private static final double BM25_B = 0.0;
	
	/**
	 * If pseudo relevance feedback is used, this is the maximum number of terms
	 * that will be used to expand the initial query.
	 */
	private static final int PRF_EXPAND = 10;
	
	/**
	 * Index handler for queries etc.
	 */
	private IndexHandler indexHandler;
	
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
	private static class Index {
		
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
		 */
		public static class TermList {
			
			/**
			 * The map of occurrences for this list's term.
			 * We use Collection instead of List because we want to use
			 * TreeSets, which do not implement the interface List. Iterate
			 * over the elements via {@link Collection#iterator()}.
			 */
			private Map<Long, Collection<Integer>> occurrences;
			
			/**
			 * Create a new TermList.
			 * Initialize the map.
			 */
			public TermList() {
				this.occurrences = new TreeMap<Long, Collection<Integer>>();
			} 
			
			/**
			 * Create a TermList from parsing a String read from an index file.
			 * @param string the string to be parsed
			 * @return a TermList
			 */
			public static TermList createFromIndexString(String string) {
				TermList list = new TermList();
				
				// format: doc:pos,pos,pos;doc:pos,pos,pos[.]
				StringTokenizer tok = new StringTokenizer(string, ":;.");
				StringTokenizer innerTok;
				Long docId = null;
				String positions = null;
				Integer position = null;
				boolean isDocId = true;
				try {
					while (tok.hasMoreTokens()) {
						String token = tok.nextToken();
						if (isDocId) {
							docId = Long.parseLong(token);
						} else {
							positions = token;
							// parse positions
							innerTok = new StringTokenizer(positions, ",");
							while (innerTok.hasMoreTokens()) {
								position = Integer.parseInt(innerTok.nextToken());
								// add occurrence to the list
								list.addOccurrence(docId, position);
							}
						}
						isDocId = !isDocId;
					}
				} catch (NumberFormatException e) {
					e.printStackTrace();
				}
				
				return list;
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
			public void addOccurrence(Long documentId, Integer position) {
				if (!this.occurrences.containsKey(documentId)) {
					this.createCollectionForDocument(documentId);
				}
				Collection<Integer> positions = this.occurrences.get(documentId);
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
					this.occurrences.put(documentId, new TreeSet<Integer>());
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
					Collection<Integer> positions = this.occurrences.get(documentId);
					if (positions != null) {
						for (int position : positions) {	// uses iterator
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
					Collection<Integer> positions = this.occurrences.get(documentId);
					if (positions != null) {
						for (int position : positions) {	// uses iterator
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
			
			public Map<Long, Collection<Integer>> getOccurrences() {
				return this.occurrences;
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
		public void addTermOccurrence(String term, Long documentId, Integer position) {
			if (!this.termLists.containsKey(term)) {
				this.createListForTerm(term);
			}
			// delegate the rest to the TermList
			this.termLists.get(term).addOccurrence(documentId, position);
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
	private static class IndexHandler {
		
		// name of the file which stores the index
		private static final String indexFileName = "index";
		// name of the file which stores the seeklist
		private static final String seekListFileName = "index_seeklist";
		// name of the file which stores the texts (for snippets)
		private static final String textsFileName = "texts";
		// name of the file which stores the seeklist for the texts
		private static final String textsSeekListFileName = "texts_seeklist";
		// name of the file which stores the mapping of document ids and titles
		private static final String titlesFileName = "titles";
		// file extension
		private static final String fileExtension = ".txt";
		// extended stopword list 
		private static final HashSet<String> GERMAN_STOP_WORDS = new HashSet<String>(
			Arrays.asList(new String[] { "a", "ab", "aber", "ach", "acht", "achte",
					"achten", "achter", "achtes", "ag", "alle", "allein", "allem", 
					"allen", "aller", "allerdings", "alles", "allgemeinen", "als", 
					"also", "am", "an", "and", "andere", "anderen", "andern", "anders", 
					"au", "auch", "auf", "aus", "ausser", "ausserdem", "b", "bald", 
					"bei", "beide", "beiden", "beim", "beispiel", "bekannt", "bereits", 
					"besonders", "besser", "besten", "bin", "bis", "bisher", "bist", 
					"c", "d", "d.h", "da", "dabei", "dadurch", "dafür", "dagegen", 
					"daher", "dahin", "dahinter", "damals", "damit", "danach", "daneben", 
					"dank", "dann", "daran", "darauf", "daraus", "darf", "darfst", "darin", 
					"darüber", "darum", "darunter", "das", "dasein", "daselbst", "dass", 
					"dasselbe", "davon", "davor", "dazu", "dazwischen", "dein", "deine", 
					"deinem", "deiner", "dem", "dementsprechend", "demgegenüber", 
					"demgemäss", "demselben", "demzufolge", "den", "denen", "denn", 
					"denselben", "der", "deren", "derjenige", "derjenigen", "dermassen", 
					"derselbe", "derselben", "des", "deshalb", "desselben", "dessen", 
					"deswegen", "dich", "die", "diejenige", "diejenigen", "dies", "diese", 
					"dieselbe", "dieselben", "diesem", "diesen", "dieser", "dieses", "dir", 
					"doch", "dort", "drei", "drin", "dritte", "dritten", "dritter", "drittes", 
					"du", "durch", "durchaus", "dürfen", "dürft", "durfte", "durften", 
					"e", "eben", "ebenso", "ehrlich", "ei", "ei,", "eigen", "eigene", "eigenen", 
					"eigener", "eigenes", "ein", "einander", "eine", "einem", "einen", "einer", 
					"eines", "einige", "einigen", "einiger", "einiges", "einmal", "eins", 
					"elf", "en", "ende", "endlich", "entweder", "er", "Ernst", "erst", 
					"erste", "ersten", "erster", "erstes", "es", "etwa", "etwas", "euch", 
					"euer", "euers", "eurem", "f", "früher", "fünf", "fünfte", "fünften", 
					"fünfter", "fünftes", "für", "g", "gab", "ganz", "ganze", "ganzen", 
					"ganzer", "ganzes", "gar", "gedurft", "gegen", "gegenüber", "gehabt", 
					"gehen", "geht", "gekannt", "gekonnt", "gemacht", "gemocht", "gemusst", 
					"genug", "gerade", "gern", "gesagt", "geschweige", "gewesen", "gewollt", 
					"geworden", "gibt", "ging", "gleich", "gott", "gross", "grosse", 
					"grossen", "grosser", "grosses", "gut", "gute", "guter", "gutes", 
					"h", "habe", "haben", "habt", "hast", "hat", "hatte", "hätte", 
					"hatten", "hätten", "heisst", "her", "heute", "hier", "hin", "hinter",
					"hoch", "i", "ich", "ihm", "ihn", "ihnen", "ihr", "ihre", "ihrem", 
					"ihren", "ihrer", "ihres", "im", "immer", "in", "indem", "infolgedessen", 
					"ins", "irgend", "ist", "j", "ja", "jahr", "jahre", "jahren", "je", 
					"jede", "jedem", "jeden", "jeder", "jedermann", "jedermanns", "jedoch", 
					"jemand", "jemandem", "jemanden", "jene", "jenem", "jenen", "jener", 
					"jenes", "jetzt", "k", "kam", "kann", "kannst", "kaum", "kein", "keine", 
					"keinem", "keinen", "keiner", "kleine", "kleinen", "kleiner", "kleines", 
					"kommen", "kommt", "können", "könnt", "konnte", "könnte", "konnten", 
					"kurz", "l", "lang", "lange", "leicht", "leide", "lieber", "los", 
					"m", "machen", "macht", "machte", "mag", "magst", "mahn", "man", 
					"manche", "manchem", "manchen", "mancher", "manches", "mann", "mehr", 
					"mein", "meine", "meinem", "meinen", "meiner", "meines", "mensch", 
					"menschen", "mich", "mir", "mit", "mittel", "mochte", "möchte", 
					"mochten", "mögen", "möglich", "mögt", "morgen", "muss", "müssen", 
					"musst", "müsst", "musste", "mussten", "n", "na", "nach", "nachdem", 
					"nahm", "natürlich", "neben", "nein", "neue", "neuen", "neun", "neunte", 
					"neunten", "neunter", "neuntes", "nicht", "nichts", "nie", "niemand", 
					"niemandem", "niemanden", "noch", "nun", "nur", "o", "ob", "oben", "oder", 
					"of", "offen", "oft", "ohne", "Ordnung", "p", "q", "r", "recht", "rechte", 
					"rechten", "rechter", "rechtes", "richtig", "rund", "s", "sa", "sache", 
					"sagt", "sagte", "sah", "satt", "schlecht", "Schluss", "schon", "sechs", 
					"sechste", "sechsten", "sechster", "sechstes", "sehr", "sei", "seid", 
					"seien", "sein", "seine", "seinem", "seinen", "seiner", "seines", "seit", 
					"seitdem", "selbst", "sich", "sie", "sieben", "siebente", "siebenten", 
					"siebenter", "siebentes", "sind", "so", "solang", "solche", "solchem", 
					"solchen", "solcher", "solches", "soll", "sollen", "sollte", "sollten", 
					"sondern", "sonst", "sowie", "später", "statt", "t", "tag", "tage", "tagen", 
					"tat", "teil", "tel", "the", "to", "tritt", "trotzdem", "tun", "u", 
					"über", "überhaupt", "übrigens", "uhr", "um", "und", "und?", "uns", 
					"unser", "unsere", "unserem", "unserer", "unsers", "unter", "v", "vergangenen", 
					"viel", "viele", "vielem", "vielen", "vielleicht", "vier", "vierte", 
					"vierten", "vierter", "viertes", "vom", "von", "vor", "w", "wahr?", 
					"während", "währenddem", "währenddessen", "wann", "war", "wäre", "waren", 
					"wart", "warum", "was", "wegen", "weil", "weit", "weiter", "weitere", 
					"weiteren", "weiteres", "welche", "welchem", "welchen", "welcher", 
					"welches", "wem", "wen", "wenig", "wenige", "weniger", "weniges", 
					"wenigstens", "wenn", "wer", "werde", "werden", "werdet", "wessen", 
					"wie", "wieder", "will", "willst", "wir", "wird", "wirklich", "wirst", 
					"wo", "wohl", "wollen", "wollt", "wollte", "wollten", "worden", "wurde", 
					"würde", "wurden", "würden", "x", "y", "z", "z.b", "zehn", "zehnte", 
					"zehnten", "zehnter", "zehntes", "zeit", "zu", "zuerst", "zugleich", 
					"zum", "zunächst", "zur", "zurück", "zusammen", "zwanzig", "zwar", 
					"zwei", "zweite", "zweiten", "zweiter", "zweites", "zwischen", "zwölf" }));
		
//		private static final int THRESHOLD = 5000;
		
		// directory of files to be read / written
		private String dir;
		// simple counter to flush index files to avoid exceedingly high memory consumption
//		private int pageCount = 0;
		
		// the analyzer for pre-processing of documents and queries
		private Analyzer analyzer;
		
		// the index
		private Index index;
		// the seeklist (term - offset)
		private Map<String, Long> seeklist;
		// the seeklist for the texts (document id - offset)
		private Map<Long, Long> textsSeeklist;
		// the mapping from document ids to titles
		private Map<Long, String> titles;
		
		/**
		 * Create an Indexer which handles index files in the given directory.
		 * This includes the creation of an analyzer for pre-processing
		 * as well as the initialization of all index structures.
		 * Uses {@link TreeMap} for maps whose keys are ordered.
		 * @param dir the directory for all index files
		 */
		public IndexHandler(String dir) {
			// delegate to more general constructor
			this(dir, false);
		}
		
		/**
		 * Create an Indexer with empty structures (load is <tt>false</tt>) or
		 * attempt to initialize the Indexer using existent index files in the
		 * given directory (load is <tt>true</tt>).
		 * @param dir the directory for all index files
		 * @param load whether to load existing index files or not
		 */
		public IndexHandler(String dir, boolean load) {
			// set references, initialize structures
			this.dir = dir;
			this.analyzer = this.createAnalyzer();
			this.index = new Index();
			this.seeklist = new TreeMap<String, Long>();
			this.textsSeeklist = new TreeMap<Long, Long>();
			this.titles = new TreeMap<Long, String>();
			// if a new index is to be created, delete old files (if necessary)
			if (!load) {
				this.deleteOldFiles();
			} else {
				// load seeklist and mapping of titles
				this.loadIndex();
			}
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
			 * so we might replace these characters (or all UTF-8 characters which
			 * are not ASCII in general) with some other representation.
			 */
			Analyzer analyzer = new GermanAnalyzer(version, CharArraySet.copy(version, GERMAN_STOP_WORDS));
			
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
			GermanStemFilter stemFilter = new GermanStemFilter(stream);
			
			// initiate the list with an initial capacity of 1000
			List<String> terms = new ArrayList<String>(1000);
			
			try {
				stemFilter.reset();	// necessary for subsequent calls to analyzer.tokenStream
				while (stemFilter.incrementToken()) {	// proceed to the next term
					terms.add(termAtt.toString());	// get the term
				}
				stemFilter.end();
			} finally {
				stemFilter.close();
			}
			
			return terms;
		}
		
		/**
		 * Add the id-title-mapping to the respective map.
		 * Add the occurrences of all terms in the given text to the index.
		 * Add the text to the texts file and store the offset.
		 * If an IOException occurs, print it, but proceed.
		 * @param id the id of the document
		 * @param title the title of the document
		 * @param text the text of the document
		 */
		public void indexPage(Long id, String title, String text) {
			// note id - title - mapping
			this.titles.put(id, title);
			
			try {
				// process text (tokenizing, stopping, stemming)
				List<String> terms = this.processRawText(text);
				// add occurrences to index
				for (int position = 0; position < terms.size(); position++) {
					String term = terms.get(position);
					this.index.addTermOccurrence(term, id, position);
				}
//				this.pageCount++;
			} catch (IOException e) {
				// an IOException was thrown by the Analyzer
				e.printStackTrace();
			}
			
//			if (this.pageCount % THRESHOLD == 0) {
//				int counter = this.pageCount / THRESHOLD;
//				try {
//					File indexFile = this.getErasedFile(this.dir 
//							+ IndexHandler.indexFileName 
//							+ counter 
//							+ IndexHandler.fileExtension);
//					/* 
//					 * get random access file for index with read / write, attempts 
//					 * to make nonexistent file
//					 */
//					FileOutputStream stream = new FileOutputStream(indexFile);
//					OutputStreamWriter streamWriter = new OutputStreamWriter(stream, "ISO-8859-1");
//					
//					// get map of terms and their occurrence lists
//					Map<String, Index.TermList> termLists = this.index.getTermLists();
//					// write each occurrence list to the file and note the offsets
//					for (String term : termLists.keySet()) {	// uses iterator
//						// write the list using custom toIndexString method of TermList
//						streamWriter.write(termLists.get(term).toIndexString());
//					}
//					// close the index file
//					streamWriter.close();
//					stream.close();
//				} catch(IOException e) {
//					e.printStackTrace();
//				}
//				this.index = new Index();
//			}
			
			// handle texts file and seeklist
			try {
				RandomAccessFile raTextsFile = new RandomAccessFile(new File(this.dir 
						+ IndexHandler.textsFileName 
						+ IndexHandler.fileExtension), "rw");
				
				// set pointer to end of file (position of first new byte to be written)
				raTextsFile.seek(raTextsFile.length());
				
				// store offset (before the text) in the seeklist of the texts file
				this.textsSeeklist.put(id, raTextsFile.getFilePointer());
				
				// remove tabs from the text and add one as separator
				String processedText = (text != null ? text : "")
						.replace('\t', ' ')
						.concat("\t");
				
				// write text of the document to the file
				raTextsFile.writeChars(processedText);
				
				// close the file
				raTextsFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		/**
		 * Merges all parts of the index.
		 * TODO: implement merging
		 * Creates the seeklist.
		 * Writes the index, the seeklist and the id-titles-mapping to 
		 * files (one file each).
		 * If an IOException occurs, print it, but proceed.
		 */
		public void createIndex() {
			try {
				// get the index file; if it does already exist, delete it
				File indexFile = this.getErasedFile(this.dir 
						+ IndexHandler.indexFileName 
						+ IndexHandler.fileExtension);
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
				 * write the seeklist to a file
				 */
				writeStringifiedToFile(this.seekListToString(), this.dir 
						+ IndexHandler.seekListFileName 
						+ IndexHandler.fileExtension);
				
				/*
				 * write the seeklist of the texts file to a file
				 */
				writeStringifiedToFile(this.textsSeekListToString(), this.dir
						+ IndexHandler.textsSeekListFileName
						+ IndexHandler.fileExtension);
				
				/*
				 * write the id-title-mapping to a file
				 */
				writeStringifiedToFile(this.titlesToString(), this.dir 
						+ IndexHandler.titlesFileName 
						+ IndexHandler.fileExtension);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void writeStringifiedToFile(String content, String filename) throws IOException {
			RandomAccessFile raFile = new RandomAccessFile(filename, "rw");
			raFile.writeChars(content);
			raFile.close();
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
		 * Stringifies the seek list for writing it to a file. Uses the pattern:<br>
		 * <i>apfel\t0\tbaum\t1608.</i><br>
		 * (\t are actual tab characters)
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
		 * Stringifies the seek list of the texts file for writing it to a file.
		 * Use the pattern like in {@link #seekListToString()}.
		 * @return a string representation of the seek list of the texts file
		 */
		private String textsSeekListToString() {
			StringBuilder result = new StringBuilder();
			
			for (Long id : this.textsSeeklist.keySet()) {	// uses iterator
				result
					.append(id)
					.append('\t')
					.append(this.textsSeeklist.get(id))
					.append('\t');
			}
			// remove the last '\t'
			result.deleteCharAt(result.lastIndexOf("\t"));
			
			return result.toString();
		}
		
		/**
		 * Stringifies the id-title-mapping for writing it to a file. Uses the pattern:
		 * 	1\tAlan Smithee\t3\tActinium	(\t are actual tab characters)
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
		
		/**
		 * Load existing seek list and id-title-mapping. If the necessary files
		 * are not present in the directory, log a message but proceed.
		 * If an IOException occurs, print it, but proceed.
		 */
		private void loadIndex() {
			try {
				// load the seek list
				File seekListFile = new File(this.dir 
						+ IndexHandler.seekListFileName 
						+ IndexHandler.fileExtension);
				RandomAccessFile raSeekListFile = new RandomAccessFile(seekListFile, "r");
				
				StringBuilder builder = new StringBuilder();
				try {
					while (true) {
						builder.append(raSeekListFile.readChar());
					}
				} catch (EOFException e) {
					// end of file: proceed
				} finally {
					raSeekListFile.close();
				}
				
				this.parseSeekListFileString(builder.toString());
				
				// load the seek list of the texts file
				File textsSeekListFile = new File(this.dir 
						+ IndexHandler.textsSeekListFileName 
						+ IndexHandler.fileExtension);
				RandomAccessFile raTextsSeekListFile = new RandomAccessFile(textsSeekListFile, "r");
				
				builder = new StringBuilder();
				try {
					while (true) {
						builder.append(raTextsSeekListFile.readChar());
					}
				} catch (EOFException e) {
					// end of file: proceed
				} finally {
					raTextsSeekListFile.close();
				}
				
				this.parseTextsSeekListFileString(builder.toString());
				
				// load the id-titles-mapping
				File titlesFile = new File(this.dir 
						+ IndexHandler.titlesFileName 
						+ IndexHandler.fileExtension);
				RandomAccessFile raTitlesFile = new RandomAccessFile(titlesFile, "rw");
				
				builder = new StringBuilder();
				try {
					while (true) {
						builder.append(raTitlesFile.readChar());
					}
				} catch (EOFException e) {
					// end of file: proceed
				} finally {
					raTitlesFile.close();
				}
				
				this.parseTitlesFileString(builder.toString());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		/**
		 * Construct a seek list from the read string. If an exception occurs,
		 * print it, but proceed.
		 * @param string seek list file string
		 */
		private void parseSeekListFileString(String string) {
			StringTokenizer tok = new StringTokenizer(string, "\t");
			
			boolean isTerm = true;	// whether the current token is the term
			String term = null;
			Long offset = null;
			
			while (tok.hasMoreTokens()) {
				String token = tok.nextToken();
				try {
					if (isTerm) {
						// parse the term
						term = token;
					} else {
						// parse the term's offset and add it to the map
						offset = Long.parseLong(token);
						this.seeklist.put(term, offset);
					}
					isTerm = !isTerm;
				} catch (NumberFormatException e) {
					e.printStackTrace();
				}
			}
		}
		
		/**
		 * Construct a seek list of the texts file from the read string. 
		 * If an exception occurs, print it, but proceed.
		 * @param string seek list file string
		 */
		private void parseTextsSeekListFileString(String string) {
			StringTokenizer tok = new StringTokenizer(string, "\t");
			
			boolean isId = true;	// whether the current token is the id
			Long id = null;
			Long offset = null;
			
			while (tok.hasMoreTokens()) {
				String token = tok.nextToken();
				try {
					if (isId) {
						// parse the term
						id = Long.parseLong(token);
					} else {
						// parse the term's offset and add it to the map
						offset = Long.parseLong(token);
						this.textsSeeklist.put(id, offset);
					}
					isId = !isId;
				} catch (NumberFormatException e) {
					e.printStackTrace();
				}
			}
		}
		
		/**
		 * Construct an id-titles-mapping from the read string. If an exception occurs,
		 * print it, but proceed.
		 * @param string seek list file string
		 */
		private void parseTitlesFileString(String string) {
			StringTokenizer tok = new StringTokenizer(string, "\t");
			
			boolean isId = true;	// whether the current token is the id
			Long id = null;
			String title = null;
			
			while (tok.hasMoreTokens()) {
				String token = tok.nextToken();
				try {
					if (isId) {
						// parse the term
						id = Long.parseLong(token);
					} else {
						// parse the term's offset and add it to the map
						title = token;
						this.titles.put(id, title);
					}
					isId = !isId;
				} catch (NumberFormatException e) {
					e.printStackTrace();
				}
			}
		}
		
		/**
		 * Delete all index files which exist (as preparation for the creation
		 * of new index files).
		 */
		private void deleteOldFiles() {
			// use getErasedFile to erase the files, if they exist
			try {
				this.getErasedFile(dir 
						+ IndexHandler.indexFileName 
						+ IndexHandler.fileExtension);
				this.getErasedFile(dir 
					+ IndexHandler.seekListFileName 
					+ IndexHandler.fileExtension);
				this.getErasedFile(dir 
					+ IndexHandler.textsFileName 
					+ IndexHandler.fileExtension);
				this.getErasedFile(dir 
					+ IndexHandler.textsSeekListFileName 
					+ IndexHandler.fileExtension);
				this.getErasedFile(dir 
					+ IndexHandler.titlesFileName 
					+ IndexHandler.fileExtension);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		/**
		 * Tests whether the given directory has all necessary index files
		 * (index file, seek list, id-title-mapping). If an IOException occurs,
		 * return <tt>false</tt>.
		 * @param dir the directory
		 * @return <tt>true</tt> if all files are present and can be accessed, 
		 * 	<tt>false</tt> otherwise
		 */
		public static boolean directoryHasIndexFiles(String dir) {
			File indexFile = new File(dir 
					+ IndexHandler.indexFileName 
					+ IndexHandler.fileExtension);
			if (!indexFile.canRead()) {
				return false;
			}
			File seekListFile = new File(dir 
					+ IndexHandler.seekListFileName 
					+ IndexHandler.fileExtension);
			if (!seekListFile.canRead()) {
				return false;
			}
			File textsFile = new File(dir 
					+ IndexHandler.textsFileName 
					+ IndexHandler.fileExtension);
			if (!textsFile.canRead()) {
				return false;
			}
			File textsSeekListFile = new File(dir 
					+ IndexHandler.textsSeekListFileName 
					+ IndexHandler.fileExtension);
			if (!textsSeekListFile.canRead()) {
				return false;
			}
			File titlesFile = new File(dir 
					+ IndexHandler.titlesFileName 
					+ IndexHandler.fileExtension);
			if (!titlesFile.canRead()) {
				return false;
			}
			// all files exist and can be read
			return true;
		}
		
		/**
		 * Use the seek list to read the inverted list of the given term from
		 * the index file.
		 * If the term (which should be pre-processed) is not found in the 
		 * seek list, or if an exception occurs, an empty list is returned.
		 * For each call to this method, a new TermList is created, so manipulating
		 * the returned object will not change any internal state of the Index
		 * or IndexHandler.
		 * @param term the term
		 * @return the read TermList
		 */
		public Index.TermList readListForTerm(String term) {
			if (term == null) {
				throw new IllegalArgumentException("term must not be null!");
			}
			if (this.seeklist.containsKey(term)) {
				// get the offset
				long offset = this.seeklist.get(term);
				try {
					// get the file
					File seekListFile = new File(this.dir + IndexHandler.indexFileName + IndexHandler.fileExtension);
					RandomAccessFile raSeekListFile = new RandomAccessFile(seekListFile, "r");
					
					/* 
					 * read the file until the list is finished (i.e., until the 
					 * first '.' or end of file
					 */
					raSeekListFile.seek(offset);
					StringBuilder builder = new StringBuilder();
					try {
						while (true) {
							String character = String.valueOf(raSeekListFile.readChar());
							builder.append(character);
							if (character.equals(".")) {
								break;
							}
						}
					} catch (EOFException e) {
						// continue
					}
					raSeekListFile.close();
					
					// create the TermList from the string
					Index.TermList list = Index.TermList.createFromIndexString(builder.toString());
					
					return list;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			// term now known or exception occurred
			return new Index.TermList();
		}
		
		/**
		 * Helper function which returns the total number of documents in the corpus.
		 * @return the total number of documents
		 */
		public int totalNumberOfDocuments() {
			return this.titles.keySet().size();
		}
		
		/**
		 * Create a snippet of the document. Look for any occurrence of a search
		 * term and retrieve the text around that occurrence.
		 * @param documentId the ID of the document
		 * @param queryTerms the terms searched for
		 * @return the snippet or <tt>null</tt> if the given id is <tt>null</tt>,
		 *   the document is not known or an error occurs
		 */
		public String getSnippetForDocumentId(Long documentId, List<String> queryTerms) {
			// catch unsuited arguments
			if (documentId == null) {
				return null;
			}
			
			// get the file offset of the texts file
			Long offset = this.textsSeeklist.get(documentId);
			if (offset == null) {
				// document is not known
				return null;
			}
			
			// read the original text of the document from the texts file
			StringBuilder builder = new StringBuilder();
			try {
				// get the file
				File textsFile = new File(this.dir + IndexHandler.textsFileName + IndexHandler.fileExtension);
				RandomAccessFile raTextsFile = new RandomAccessFile(textsFile, "r");
				
				/* 
				 * read the file until the text is finished (i.e., until the 
				 * first '\t' or end of file
				 */
				raTextsFile.seek(offset);
				try {
					while (true) {
						String character = String.valueOf(raTextsFile.readChar());
						if (character.equals("\t")) {
							break;
						} else {
							builder.append(character);
						}
					}
				} catch (EOFException e) {
					// continue
				}
				raTextsFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			String text = builder.toString();
			if (text == null || text.equals("")) {
				// no text
				return null;
			}
			
			/*
			 * Create a snippet from the text.
			 * TODO: consider query terms; this is difficult because they are
			 * pre-processed (which means that the text would have to be pre-
			 * processed as well, which makes creating the snippet difficult)
			 */
			// simple algorithm: just use the beginning
			int endIndex = Math.min(240, text.length());				// exclusive end index
			int lastSpaceIndex = text.lastIndexOf(" ", (endIndex - 1));	// searching backwards
			if (lastSpaceIndex > 180) {
				endIndex = lastSpaceIndex;
			}
			return text.substring(0, endIndex)	// endIndex cannot be 0 due to a former test of text
					.replace('\n', ' ')			// remove newlines
					+ "...";
		}
		
	}
	
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
	private class SAXHandler extends DefaultHandler {

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
		
		public SAXHandler(IndexHandler indexer) {
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
	
	/**
	 * Parse the dump and create the index in the given "dir".
	 * The dump may be located elsewhere.
	 * @param dir the path of the directory; an '/' will be appended, if necessary
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

		/* 
		 * create the indexer with the target dir; this instance is only used for
		 * creating the index, not for answering queries
		 */
		IndexHandler indexer = new IndexHandler(dir);
		
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
	boolean loadIndex(String dir) {
		if (dir == null) {
			this.log("abort: dir is null");
			return false;
		}
		
		// ensure that the dir path ends with a '/'
		if (!dir.endsWith("/")) {
			dir = dir.concat("/");
		}
		
		// test whether the given directory has all necessary index files
		if (!IndexHandler.directoryHasIndexFiles(dir)) {
			// missing file(s): return false
			return false;
		}
		// all files present => load index
		this.indexHandler = new IndexHandler(dir, true);
		return true;
	}

	@Override
	ArrayList<String> search(String query, int topK, int prf) {
		// invalid arguments: return an empty result set
		if (query == null || topK <= 0) {
			return new ArrayList<String>();
		}
		
		// answer query depending on its type TODO: make sure that topK and prf are used if necessary
		if (isBooleanQuery(query)) {
			return processBooleanQuery(query);			// boolean query
		} else if (query.contains("*")) {
			return processPrefixQuery(query);			// prefix query
		} else if (query.contains("'") || query.contains("\"")) {
			return processPhraseQuery(query);			// phrase query
		} else {
			return processBM25Query(query, topK, prf);	// keyword query
		}
	}
	
	/**
	 * Creates a list of String representations for the documents denoted by the
	 * given IDs.<br>
	 * For each document, the title followed by a snippet is returned.<br>
	 * If the given list is <tt>null</tt> or empty, an empty list is returned.
	 * @param documentIds the IDs of the relevant documents
	 * @param queryTerms the list of terms used in the query, used for snippet
	 *   creation
	 * @return a list of String representations of the relevant documents which
	 * 	is never <tt>null</tt>
	 */
	private ArrayList<String> createQueryAnswerForDocuments(
			List<Long> documentIds, List<String> queryTerms) {
		// catch unsuited arguments
		if (documentIds == null || documentIds.size() <= 0) {
			return new ArrayList<String>();
		}
		
		ArrayList<String> result = new ArrayList<String>(documentIds.size());
		
		for (Long documentId : documentIds) {
			// get the title of the document
			String title = this.indexHandler.titles.get(documentId);
			// get a snippet of the document
			String snippet = this.indexHandler.getSnippetForDocumentId(documentId, queryTerms);
			
			// store: title + newline (unless snippet is null) + snippet
			result.add((title != null ? title : "") 
					+ (snippet != null ? ("\n" + snippet) : ""));
		}
		
		return result;
	}
	
	/**
	 * Check if a given query is a boolean query (i.e., contains at least one
	 * boolean operator).
	 * @param query the query text
	 * @return <tt>true</tt> if the query is a boolean query, 
	 * 		<tt>false</tt> otherwise
	 */
	private boolean isBooleanQuery(String query) {
		query = query.toUpperCase();	// "and" => "AND"
		
		for (String operator : SearchEngineRetrEvil.BOOLEAN_OPERATORS) {
			if (query.contains(operator)) {
				return true;
			}
		}
		
		return false;
	}
	
	/**
	 * Process the query as a prefix query, i.e., find documents containing
	 * any indexed term which starts with the given prefix.
	 * @param query the query text
	 * @return a list of titles of relevant documents
	 */
	private ArrayList<String> processPrefixQuery(String query) {
		String prefix = query.substring(0, query.indexOf("*"));
		Set<String> results = new TreeSet<String>();
		for(Entry<String, Long> entry : this.indexHandler.seeklist.entrySet()) {
			if(entry.getKey().startsWith(prefix))
				results.add(this.indexHandler.titles.get(entry.getValue()));
		}
		return new ArrayList<String>(results);
	}
	
	/**
	 * Overly simplistic boolean query processing assuming the query operator 
	 * sits between two search terms.
	 * TODO: merging keySet()s does not work properly (NullPointerException; not beautiful)
	 * TODO: merge boolean and phrase query handling (”Art* BUT NOT Artikel”)
	 * @param query the query text
	 * @return a list of titles of relevant documents
	 */
	private ArrayList<String> processBooleanQuery(String query) {
		Set<String> results = new TreeSet<String>();
		
		query = query.toUpperCase();	// "and" => "AND"
		
		String[] terms = query.split(" ");
		if (query.contains(SearchEngineRetrEvil.AND)) {
			int index = Arrays.asList(terms).indexOf(SearchEngineRetrEvil.AND);
			String leftProcessedTerm = processBooleanQuery(Arrays.asList(terms).get(index - 1)).get(0);
			String rightProcessedTerm = processBooleanQuery(Arrays.asList(terms).get(index + 1)).get(0);
			
			Index.TermList leftTermList = this.indexHandler.readListForTerm(leftProcessedTerm);
			Index.TermList rightTermList = this.indexHandler.readListForTerm(rightProcessedTerm);
			for (Long document : leftTermList.getOccurrences().keySet()) {
				if (rightTermList.getOccurrences().containsKey(document)) {
					results.add(this.indexHandler.titles.get(document));
				}
			}
		} else if (query.contains(SearchEngineRetrEvil.OR)) {
			int index = Arrays.asList(terms).indexOf(SearchEngineRetrEvil.OR);
			String leftProcessedTerm = processBooleanQuery(Arrays.asList(terms).get(index - 1)).get(0);
			String rightProcessedTerm = processBooleanQuery(Arrays.asList(terms).get(index + 1)).get(0);
			
			Index.TermList leftTermList = this.indexHandler.readListForTerm(leftProcessedTerm);
			Index.TermList rightTermList = this.indexHandler.readListForTerm(rightProcessedTerm);
			leftTermList.getOccurrences().keySet().addAll(rightTermList.getOccurrences().keySet());
			for (Long document : leftTermList.getOccurrences().keySet()) {
				results.add(this.indexHandler.titles.get(document));
			}
		} else if (query.contains(SearchEngineRetrEvil.BUT_NOT)) {
			int index = Arrays.asList(terms).indexOf(SearchEngineRetrEvil.BUT_NOT);
			String leftProcessedTerm = processBooleanQuery(Arrays.asList(terms).get(index - 1)).get(0);
			String rightProcessedTerm = processBooleanQuery(Arrays.asList(terms).get(index + 1)).get(0);
			
			Index.TermList leftTermList = this.indexHandler.readListForTerm(leftProcessedTerm);
			Index.TermList rightTermList = this.indexHandler.readListForTerm(rightProcessedTerm);
			for (Long document : leftTermList.getOccurrences().keySet()) {
				if (!rightTermList.getOccurrences().containsKey(document)) {
					results.add(this.indexHandler.titles.get(document));
				}
			}
		} else {
			try {
				return (ArrayList<String>) this.indexHandler.processRawText(query);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return new ArrayList<String>(results);
	}
	
	/**
	 * TODO: implement
	 * @param query
	 * @return
	 */
	private ArrayList<String> processPhraseQuery(String query) {
		Set<String> results = new TreeSet<String>();
//		int firstIndex = query.indexOf("'") < query.indexOf("\"") ? query.indexOf("'") : query.indexOf("\"");
//		int lastIndex = query.lastIndexOf("'") > query.lastIndexOf("\"") ? query.lastIndexOf("'") : query.lastIndexOf("\"");
//		String content = query.substring(firstIndex, lastIndex);
//		String[] terms = content.split(" ");
//		List<Index.TermList> termLists = new ArrayList<Index.TermList>();
//		for(String term : terms) {
//			termLists.add(this.indexHandler.readListForTerm(term));
//		}
//		for(Index.TermList list : termLists) {
//			for(Entry<Long, Collection<Integer>> occurrence : list.occurrences.entrySet()) {
//				if(occurrence.)
//			}
//		}
		return new ArrayList<String>(results);
	}
	
	/**
	 * This method is <tt>obsolete</tt>. Use {@link #processBM25Query(String, int)}
	 * instead.<br>
	 * 
	 * Process the query as a simple keyword query, i.e., pre-process it and
	 * treat every term as a search term which must be present.
	 * @param query the query text
	 * @return a list of titles of relevant documents
	 */
	@SuppressWarnings("unused")
	private ArrayList<String> processKeywordQuery(String query) {
		// prepare array of titles
		ArrayList<String> titles = new ArrayList<String>();
		
		try {
			// pre-process the query
			List<String> terms = this.indexHandler.processRawText(query);
			
			// find occurrences of the keyword (if there is any)
			Index.TermList termList = terms.size() > 0 
					? this.indexHandler.readListForTerm(terms.get(0)) 
					: null;
			
			// get document titles for the document id(s)
			Set<Long> documentIds = termList.getOccurrences().keySet();
			for (Long documentId : documentIds) {
				String title = this.indexHandler.titles.get(documentId);
				titles.add(title != null ? title : "- title for id " + documentId + " -");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// return the titles
		return titles;
	}
	
	/**
	 * Process the query as a keyword query which is handled using the 
	 * probabilistic model BM25. Returns a maximum of <tt>topK</tt> titles
	 * of documents ordered by rank (highest rank first).<br><br>
	 * 
	 * There is <b>no</b> relevance information about documents, so the corresponding
	 * variables <tt>r</tt> and <tt>R</tt> are treated as 0.<br>
	 * Uses the parameters {@link #BM25_K1}, {@link #BM25_K2}, {@link #BM25_B}
	 * to regulate the weighting of term frequencies.<br><br>
	 * 
	 * If <tt>prf</tt> is greater than <tt>0</tt>, pseudo relevance feedback is
	 * used.
	 * @param query the query text
	 * @param topK the maximum number of titles to return
	 * @param prf use pseudo relevance feedback using the top <tt>prf</tt> documents
	 *   (if it is <tt>0</tt>, no pseudo relevance feedback is used)
	 * @return a list of titles of ranked documents
	 */
	private ArrayList<String> processBM25Query(String query, int topK, int prf) {
		ArrayList<String> result = new ArrayList<String>();
		
		try {
			// pre-process the query to get the query terms
			List<String> terms = this.indexHandler.processRawText(query);
			
			if (prf == 0) {
				// no pseudo relevance feedback
				
				// get the IDs of the topK most relevant documents
				ArrayList<Long> ids = this.processInnerBM25Query(terms, topK);
				
				result = this.createQueryAnswerForDocuments(ids, terms);
			} else {
				// pseudo relevance feedback
				
				// get the IDs of the prf most relevant documents
				ArrayList<Long> ids = this.processInnerBM25Query(terms, prf);
				
				// get the snippets
				ArrayList<String> snippets = this.createQueryAnswerForDocuments(ids, terms);
				
				// use the snippets to expand the query
				terms = this.expandQueryTerms(terms, snippets);
				
				// reevaluate the expanded query and get the topK most relevant documents
				ids = this.processInnerBM25Query(terms, topK);
				
				result = this.createQueryAnswerForDocuments(ids, terms);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * Helper method to expand the given (pre-processed) query terms using the
	 * snippets of relevant documents as part of the integration of pseudo 
	 * relevance feedback into the query engine.
	 * @param terms query terms of the initial query
	 * @param snippets snippets of documents which were relevant for the initial query
	 * @return an enhanced list of query terms for the next query
	 */
	private ArrayList<String> expandQueryTerms(List<String> terms, List<String> snippets) throws IOException {
		ArrayList<String> expandedTerms = new ArrayList<String>();
		
		// add initial terms
		expandedTerms.addAll(terms);
		
		// pre-process snippets
		ArrayList<List<String>> processedSnippets = new ArrayList<List<String>>(snippets.size());
		for (String snippet : snippets) {
			processedSnippets.add(this.indexHandler.processRawText(snippet));
		}
		
		// count terms in all snippets
		Map<String, Double> termCountMap = new TreeMap<String, Double>();
		for (List<String> processedSnippet : processedSnippets) {
			for (String term : processedSnippet) {
				if (expandedTerms.contains(term)) {
					// term is already used
					continue;
				}
				if (termCountMap.containsKey(term)) {
					// increment count
					termCountMap.put(term, termCountMap.get(term) + 1.0);
				} else {
					// initial count of 1
					termCountMap.put(term, 1.0);
				}
			}
		}
		
		// order by count, slightly change count if necessary to avoid overrides
		Map<Double, String> sortedTerms = new TreeMap<Double, String>();
		for (String term : termCountMap.keySet()) {
			Double count = termCountMap.get(term);
			while (sortedTerms.containsKey(count)) {
				count -= 1e-10;
			}
			sortedTerms.put(count, term);
		}
		
		// get the reverse order of counts
		List<Double> reverseCounts = new ArrayList<Double>();
		for (Double key : sortedTerms.keySet()) {	// keySet of a TreeMap is ordered
			reverseCounts.add(0, key);
		}
		
		// expand the query terms using a particular maximum of new terms
		int newTermsCount = 0;
		for (Double key : reverseCounts) {
			expandedTerms.add(sortedTerms.get(key));
			newTermsCount++;
			if (newTermsCount >= PRF_EXPAND) {
				break;
			}
		}

		return expandedTerms;
	}
	
	/**
	 * Helper method to perform the actual BM25 query, see 
	 * {@link #processBM25Query(String, int, int)}.
	 */
	private ArrayList<Long> processInnerBM25Query(List<String> terms, int topK) {
		ArrayList<Long> result = new ArrayList<Long>();

		// if there are no terms, return an empty result set
		if (terms.size() == 0) {
			return result;
		}

		// read index file: get lists of occurrences for all query terms
		Map<String, Index.TermList> termListMap = new HashMap<String, Index.TermList>();
		for (String term : terms) {
			if (termListMap.containsKey(term)) {
				continue;	// already got the list for this term
			}
			// add (term, termList) to the map; termList is never null
			termListMap.put(term, this.indexHandler.readListForTerm(term));
		}

		/*
		 * Compute variable n (number of documents containing a term) per 
		 * term by parsing the occurrences. Use this opportunity to compute
		 * variable qf (frequency of term in the query) per term. Also get the
		 * set of all documents containing any query term.
		 */
		Map<String, Integer> termDocumentCountMap = new HashMap<String, Integer>();
		Map<String, Integer> termQueryFrequency = new HashMap<String, Integer>();
		Set<Long> documentIds = new HashSet<Long>();	// HashSet: no repetitions
		for (String term : terms) {
			// increment frequency
			Integer frequency = termQueryFrequency.get(term);	// null if not set yet
			termQueryFrequency.put(term, frequency != null ? frequency + 1 : 1);

			/*
			 * Get ids of documents in which the term occurs as well as the
			 * number of documents containing the term (once per term)
			 */
			if (!termDocumentCountMap.containsKey(term)) {
				// add the set of document ids
				documentIds.addAll(termListMap.get(term).getOccurrences().keySet());
				// put the document ids count
				Integer documentCount = termListMap.get(term)	// termList is never null, even if the term is unknown
						.getOccurrences()						// never null either, as initialized at creation
						.keySet()								// dito
						.size();								// >= 0
				termDocumentCountMap.put(term, documentCount);
			}
		}

		// get N (the total number of documents)
		final int N = this.indexHandler.totalNumberOfDocuments();

		// get the set of query terms (without duplicates)
		Set<String> uniqueTerms = new HashSet<String>();
		uniqueTerms.addAll(terms);

		// Rank each document which contains at least one query term
		Map<Double, Long> scoreDocumentMap = new TreeMap<Double, Long>();	// ordered by score
		for (Long documentId : documentIds) {
			Double score = 0.0;	// score of this document

			/*
			 * Compute K (length normalization parameter).
			 * Note that this would normally be more complicated, including
			 * the document length and average document length, but as long 
			 * as BM25_B is set to 0, that does not matter.
			 */
			double K = BM25_K1 * (1 - BM25_B);

			// compute and add the score for each query term
			for (String term : uniqueTerms) {
				// R, r = 0
				// n: number of documents containing the term
				int n = termDocumentCountMap.get(term);		// must not be null
				// f: frequency of the term in the document
				// get the positions of this term for this document
				Collection<Integer> termDocPositions = termListMap
						.get(term)							// never null, even if term is not known
						.getOccurrences()					// never null, at least initialized
						.get(documentId);					// may be null
				// if there are positions, get their count
				int f = termDocPositions != null 
						? termDocPositions.size() 
								: 0;
						// qf: frequency of the term in the query
						int qf = termQueryFrequency.get(term);  	// must not be null

						/*
						 * Compute the score. Note: for very few documents, the first
						 * factor (log ...) can be 0 (when ... is 1). Here, the natural
						 * logarithm is used (because Math offers it), but the base
						 * does not really matter.
						 */
						score += (Math.log(1.0 / ((n + 0.5) / ((N - n) + 0.5)))
								* (((BM25_K1 + 1.0) * f) / (K + f))
								* (((BM25_K2 + 1.0) * qf) / (BM25_K2 + qf)));
			}

			// make sure that the scores are unique to avoid problems with the map
			while (scoreDocumentMap.containsKey(score)) {
				score -= 1e-10;	// slightly decrease the score (this is sloppy, but works)
			}

			// store the score
			scoreDocumentMap.put(score, documentId);
		}

		// get the scores in descending order
		List<Double> descendingScores = new ArrayList<Double>();
		for (Double score : scoreDocumentMap.keySet()) {	// uses iterator
			descendingScores.add(0, score);
		}

		// get the IDs of the topK best documents
		for (int i = 0; i < topK; i++) {
			if (i < descendingScores.size()) {
				Double score = descendingScores.get(i);
				result.add(scoreDocumentMap.get(score));
			} else {
				break;
			}
		}

		return result;
	}

	@Override
	Double computeNdcg(String query, ArrayList<String> ranking, int ndcgAt) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
