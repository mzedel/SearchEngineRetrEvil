package de.hpi.krestel.mySearchEngine;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
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
		private static final String seekListFileName = "seeklist";
		// name of the file which stores the mapping of document ids and titles
		private static final String titlesFileName = "titles";
		// file extension
		private static final String fileExtension = ".txt";
		/** extended stopword list 
		* based on: http://members.unine.ch/jacques.savoy/clef/
		* and http://codingwiththomas.blogspot.de/2012/01/german-stop-words.html
		*/
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
		
		private static final int THRESHOLD = 5000;
		
		// directory of files to be read / written
		private String dir;
		// simple counter to flush index files to avoid exceedingly high memory consumption
		private int pageCount = 0;
		
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
			this.titles = new TreeMap<Long, String>();
			// load seeklist and mapping of titles, if so desired
			if (load) {
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
			 * TODO: there may be a problem with some German characters ('ü' etc.),
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
				this.pageCount++;
			} catch (IOException e) {
				// an IOException was thrown by the Analyzer
				e.printStackTrace();
			}
			
			if(this.pageCount % THRESHOLD == 0) {
				try {
					File indexFile = new File(this.dir
							+ IndexHandler.indexFileName
							+ IndexHandler.fileExtension);

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
				} catch(IOException e) {
					e.printStackTrace();
				}
				this.index = new Index();
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
//				File indexFile = this.getErasedFile(this.dir 
//						+ IndexHandler.indexFileName 
//						+ IndexHandler.fileExtension);
//				/* 
//				 * get random access file for index with read / write, attempts 
//				 * to make nonexistent file
//				 */
//				RandomAccessFile raIndexFile = new RandomAccessFile(indexFile, "rw");
//				
//				// get map of terms and their occurrence lists
//				Map<String, Index.TermList> termLists = this.index.getTermLists();
//				// write each occurrence list to the file and note the offsets
//				for (String term : termLists.keySet()) {	// uses iterator
//					// note the offset
//					long offset = raIndexFile.getFilePointer();
//					this.seeklist.put(term, offset);
//					// write the list using custom toIndexString method of TermList
//					raIndexFile.writeChars(termLists.get(term).toIndexString());
//				}
//				// close the index file
//				raIndexFile.close();
				
				/*
				 * write the seeklist to a file
				 */
				writeStringifiedToFile(this.seekListToString(), this.dir 
						+ IndexHandler.seekListFileName 
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
		// if query is null, return an empty result set
		if (query == null) {
			return new ArrayList<String>();
		}
		
		// answer query depending on its type
		if (isBooleanQuery(query)) {
			return processBooleanQuery(query);	// boolean query
		} else if (query.contains("*")) {
			return processPrefixQuery(query);	// prefix query
		} else if (query.contains("'") || query.contains("\"")) {
			return processPhraseQuery(query);	// phrase query
		} else {
			return processKeywordQuery(query);	// default: keyword query
		}
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
	 * Process the query as a simple keyword query, i.e., pre-process it and
	 * treat every term as a search term which must be present.
	 * @param query the query text
	 * @return a list of titles of relevant documents
	 */
	private ArrayList<String> processKeywordQuery(String query) {
		// prepare array of titles
		ArrayList<String> titles = new ArrayList<String>();
		
		try {
			// preprocess the query
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

	@Override
	Double computeNdcg(String query, ArrayList<String> ranking, int ndcgAt) {
		// TODO Auto-generated method stub
		return null;
	}
}
