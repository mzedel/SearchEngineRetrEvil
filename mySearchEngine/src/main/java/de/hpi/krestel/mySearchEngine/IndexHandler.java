package de.hpi.krestel.mySearchEngine;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.DatatypeConverter;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.de.GermanStemFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

/**
 * Handles everything related to the index. Deals with related I/O.
 * Gets id, title and text per article from the SAXHandler.
 * Merges everything into the final index file once the SAXHandler has
 * finished parsing.
 * (in the future:) Provides information for the query engine.
 */
class IndexHandler {

	// just to provide a simple way to switch between full index creation and just merging
	public static final boolean DEV_MODE = true;

	// name of the file which stores the index
	private static final String indexFileName = "index";
	// name of the file which stores the seeklist
	private static final String seekListFileName = "index_seeklist";
	// name of the file which stores the link index
	private static final String linkIndexFileName = "link_index";
	// name of the file which stores the texts (for snippets)
	private static final String textsFileName = "texts";
	// name of the file which stores the seeklist for the texts
	private static final String textsSeekListFileName = "texts_seeklist";
	// name of the file which stores the mapping of document ids and titles
	private static final String titlesFileName = "idsToTitles";
	// name of the file which stores the mapping of (processed) titles to ids
	private static final String titlesToIdsFileName = "titlesToIds";
	// file extension
	private static final String fileExtension = ".txt";
	// file extension
	private static final String tempFileExtension = ".tmp";

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

	/*
	 * Insertion-ordered map of regular expressions which are used to remove
	 * the syntax from documents in order to create nice snippets.
	 * Lists (* ..., # ...) are not removed.
	 */
	private static final Map<String, String> cleaningPatterns = new LinkedHashMap<String, String>();
	static {
		cleaningPatterns.put("'''", "");						// bold
		cleaningPatterns.put("''", "");							// italic
		cleaningPatterns.put("==+", "");						// headings
		cleaningPatterns.put("\\[\\[Datei:[^\\]]*\\]\\]", "");	// files (delete content)
		cleaningPatterns.put("\\[\\[[^|\\]]+\\|", "");			// internal links
		cleaningPatterns.put("\\[\\[", "");
		cleaningPatterns.put("\\]\\]", "");
		cleaningPatterns.put("\\[\\w+://[^\\s]+\\s", "");		// external links
		cleaningPatterns.put("\\[", "");
		cleaningPatterns.put("\\]", "");
		cleaningPatterns.put("\\{\\{[^}]*\\}\\}", "");			// special internal links (delete content)
		cleaningPatterns.put("\\{\\{", "");
		cleaningPatterns.put("\\}\\}", "");
		cleaningPatterns.put("\\{[^}]*\\}", "");				// templates (delete content)
		cleaningPatterns.put("\\|(.*)\n", "");
		cleaningPatterns.put("<gallery>[^<]*</gallery>", "");	// galleries (delete content)
		cleaningPatterns.put("<ref>[^<]*</ref>", "");			// references (delete content)
		cleaningPatterns.put("#WEITERLEITUNG", "");				// redirection (should not happen anyway)
		cleaningPatterns.put("<[^>]*>", "");					// arbitrary tags
		cleaningPatterns.put("\n(.*):\\\\mathrm(.*)\n", "\n");	// formulas
		cleaningPatterns.put("  ", "");							// double spaces
		cleaningPatterns.put("\n ", "\n");						// newline followed by space
		cleaningPatterns.put("\n\n\n", "\n\n");					// triple newlines
		cleaningPatterns.put("&nbsp;", " ");					// HTML whitespace
	}

	/*
	 * List of regular expressions which are used to extract internal links
	 * from pages. Patterns are immutable, Matchers are not. Do not include
	 * links to categories. Remove hash parts (e.g. ...#Section).
	 */
	private static final List<Pattern> linkingPatterns = new ArrayList<Pattern>();
	static {
		// simple internal links, e.g. [[Actinium]]
		linkingPatterns.add(Pattern.compile("\\[\\[([^:#|\\]]+)(#[^|\\]]*)?\\]\\]"));
		// internal links with text, e.g. [[Actinium|ein dummes Element]]
		linkingPatterns.add(Pattern.compile("\\[\\[([^:#|\\]]+)(#[^|\\]]*)?\\|[^\\]]*\\]\\]"));
		// external links which should be internal, e.g. 
		// [http://de.wikipedia.org/wiki/Actinium Lol voll die "externe" Seite trololol]
		linkingPatterns.add(Pattern.compile("\\[http://de\\.wikipedia\\.org/wiki/([^ :#\\]]+)[^\\]]*\\]"));
	}

	private static int THRESHOLD = 160 * 1024 * 1024;
	private int byteCounter = 0;

	// directory of files to be read / written
	private String dir;

	// simple counter to flush index files to avoid exceedingly high memory consumption
	private int fileCount = 0;

	// the analyzer for pre-processing of documents and queries
	private Analyzer analyzer;

	// the index
	private Index index;
	// the seeklist (term - offset)
	private Map<String, Long> seeklist;
	// the link index
	private LinkIndex linkIndex;
	// the seeklist for the texts (document id - offset)
	private Map<Long, Long> textsSeeklist;
	// the mapping from document ids to titles
	private Map<Long, String> idsToTitles;
	// the mapping from (processed) document titles to ids
	private Map<String, Long> titlesToIds;
	// the RandomAccessFile where the index will be written to
	private File indexFile;
	private RandomAccessFile raIndexFile;
	private FileOutputStream fos;
	private BufferedOutputStream bo;

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
		this.indexFile = new File(this.dir
				+ IndexHandler.indexFileName
				+ IndexHandler.fileExtension);
		try {
			this.raIndexFile = new RandomAccessFile(indexFile, "rw");
			this.fos = new FileOutputStream(raIndexFile.getFD());
			this.bo = new BufferedOutputStream(fos);
		}
		catch (FileNotFoundException e) { e.printStackTrace(); }
		catch (IOException e) { e.printStackTrace(); }

		this.setLinkIndex(new LinkIndex());

		this.seeklist = new TreeMap<String, Long>();
		this.textsSeeklist = new TreeMap<Long, Long>();
		this.idsToTitles = new TreeMap<Long, String>();
		this.setTitlesToIds(new TreeMap<String, Long>());

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
	List<String> processRawText(String text) throws IOException {
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
	 * Add the occurrences of links to other pages to the index.
	 * Add the text to the texts file and store the offset.
	 * If an IOException occurs, print it, but proceed.
	 * @param id the id of the document
	 * @param title the title of the document
	 * @param text the text of the document
	 */
	public void indexPage(final Long id, final String title, final String text) {
		// id - title - mapping
		this.idsToTitles.put(id, title);
		this.getTitlesToIds().put(LinkIndex.processTitle(title), id);

		// indexing
		try {
			/*
			 * Indexing of Links
			 */
			List<String> linkedDocumentTitles = this.getLinkedDocumentTitles(text);
			for (String linkedTitle : linkedDocumentTitles) {
				if (linkedTitle != null && linkedTitle.length() > 0) {
					// add linking to the linkIndex
					this.getLinkIndex().addLinkingTitle(linkedTitle, title);
					// if threshold is reached: write part of the index
					this.byteCounter += (title.length() + linkedTitle.length());
					if (this.byteCounter >= THRESHOLD) {
						writeToIndexFile();
						this.byteCounter = 0;
					}
				}
			}

			/*
			 * Indexing of Terms
			 */
			// process text (tokenizing, stopping, stemming)
			List<String> terms = this.processRawText(text);
			// add occurrences to index
			for (Integer position = 0; position < terms.size(); position++) {
				String term = terms.get(position);
				this.index.addTermOccurrence(term, id, position);
				// if threshold is reached: write part of the index
				this.byteCounter += (term.length() + id.toString().length() + position.toString().length());
				if (this.byteCounter >= THRESHOLD) {
					writeToIndexFile();
					this.byteCounter = 0;
				}
			}
		} catch (IOException e) {
			// an IOException was thrown by the Analyzer
			e.printStackTrace();
		}

		// texts file and its seeklist
		try {
			RandomAccessFile raTextsFile = new RandomAccessFile(new File(this.dir 
					+ IndexHandler.textsFileName 
					+ IndexHandler.fileExtension), "rw");

			// set pointer to end of file (position of first new byte to be written)
			raTextsFile.seek(raTextsFile.length());

			// store offset (before the text) in the seeklist of the texts file
			this.textsSeeklist.put(id, raTextsFile.getFilePointer());

			// write clean text of the document to the file (2 bytes per char)
			raTextsFile.writeChars(cleanPageText(text));;

			// close the file
			raTextsFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Parse the given text for links to other Wikipedia pages and return
	 * a list of their titles.
	 * @param wikiText the original text of the page (in Wikipedia syntax)
	 * @return a list of titles (may be empty but not <tt>null</tt>)
	 */
	private List<String> getLinkedDocumentTitles(String wikiText) {
		List<String> titles = new ArrayList<String>();

		// apply regular expressions to find links to other pages
		for (Pattern pattern : linkingPatterns) {
			// create the matcher for this pattern
			Matcher matcher = pattern.matcher(wikiText);
			// iterate over matching regions
			while (matcher.find()) {
				// add the content of the first matching group (page title)
				String title = matcher.group(1);
				if (!titles.contains(title)) {
					titles.add(title);
				}
			}
		}

		return titles;
	}

	/**
	 * Prepare the given text of a document for snippet creation, i.e., 
	 * remove all markup. Also, prepare it for being written to the texts
	 * file, i.e., replace tabs in the text and append a tab to it as 
	 * delimiter.
	 * @param text the text of a document
	 * @return text prepared for the texts file
	 */
	private String cleanPageText(String text) {
		// remove tabs from the text and add one as separator
		String processedText = (text != null ? text : "")
				.replace('\t', ' ')
				.concat("\t");

		// remove matches
		for (String pattern : cleaningPatterns.keySet()) {
			processedText = processedText.replaceAll(pattern, cleaningPatterns.get(pattern));
		}

		// return with removed leading / trailing whitespace
		return processedText.trim();
	}

	private void writeToIndexFile() {
		this.indexFile = new File(this.dir
				+ IndexHandler.indexFileName
				+ "_"
				+ this.fileCount
				+ IndexHandler.tempFileExtension);

		try {
			/*
			 * write part of index
			 */				
			this.raIndexFile = new RandomAccessFile(indexFile, "rw");
			this.fos = new FileOutputStream(raIndexFile.getFD());
			this.bo = new BufferedOutputStream(fos, 8192);

			// get map of terms and their occurrence lists
			Map<String, Index.TermList> termLists = this.index.getTermLists();
			// write each occurrence list to the file
			for (String term : termLists.keySet()) {	// uses iterator
				// write the list using custom toIndexString method of TermList
				termLists.get(term).toIndexString(this.bo, term, true);
			}

			this.fileCount++;
			this.bo.close();
			this.fos.close();

			/*
			 * write part of link index
			 * 
			 * TODO: implement; I did not implement splitting / merging
			 */

		} catch(IOException e) {
			e.printStackTrace();
		}

		this.index = new Index();
	}

	/**
	 * Merges all parts of the index.
	 * Merges all parts of the link index.
	 * Creates the seeklist.
	 * Writes the index, the seeklist, the id-titles-mapping and the 
	 * titles-id-mapping to files (one file each).
	 * If an IOException occurs, print it, but proceed.
	 */
	@SuppressWarnings("resource")
	public void createIndex() {
		try {
			/*
			 * write remaining parts of the index and link index
			 */
			writeToIndexFile();

			/*
			 * merge index files
			 */
			FilenameFilter filter = new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.endsWith(IndexHandler.tempFileExtension);
				}
			};

			File directory = new File(this.dir);
			this.indexFile = new File(this.dir
					+ IndexHandler.indexFileName
					+ IndexHandler.fileExtension);
			this.raIndexFile = new RandomAccessFile(this.indexFile, "rw");
			this.fos = new FileOutputStream(this.raIndexFile.getFD());
			this.bo = new BufferedOutputStream(this.fos, 8192);
			File[] filesInFolder = directory.listFiles(filter);
			BufferedReader[] fileBeginnings = new BufferedReader[filesInFolder.length];

			int index = 0;
			int fileCount = filesInFolder.length;
			String line = "";
			String[] terms = new String[fileCount];
			String[] lines = new String[fileCount];

			/*
			 * setup merging tools: a BufferedReader for each of the files +
			 * read the first lines each to determine what terms to merge
			 * - lines and terms are considered separately to allow colons, etc.
			 * in terms
			 */
			for(File fileEntry : filesInFolder) {
				FileReader reader = new FileReader(fileEntry);
				BufferedReader breed = new BufferedReader(reader);
				fileBeginnings[index] = breed;
				line = breed.readLine();
				terms[index] = new String(DatatypeConverter.parseBase64Binary(line.substring(0, line.indexOf(":"))));
				lines[index] = line.substring(line.indexOf(":"));
				index++;
			}
			String term = getLowest(terms);
			String nextTerm = term;
			index = 0;
			int countDown = fileCount;
			int winnerSlot = -1;
			line = "";
			/*
			 * whenever a term is merged the next line from the file it originated from is read
			 * this is continued until all lines in all files are read / all readers reached the end of the file
			 * lines of files will be merged whenever a read line yields the same term  
			 */
			while(countDown > 0) {
				for(index = 0; index < fileCount; index++) {
					if (lines[index] == null) continue;
					String currentTerm = terms[index];
					if (term.compareTo(currentTerm) == 0) {
						winnerSlot = index;
						if (line.length() > 0) line += ";";
						String toAppend = lines[index].substring(1, lines[index].lastIndexOf("."));
						line += toAppend; 
						String currentLine = fileBeginnings[index].readLine(); 
						if (currentLine == null) {
							fileBeginnings[index].close();
							fileBeginnings[index] = null;
							terms[index] = null;
							lines[index] = null;
							countDown--;
							term = getLowest(lines);
							term = term.substring(0, term.indexOf(":"));
						} else {
							terms[index] = new String(DatatypeConverter.parseBase64Binary(currentLine.substring(0, currentLine.indexOf(":"))));
							lines[index] = currentLine.substring(currentLine.indexOf(":"));
						}
					} else if (term.compareTo(currentTerm) < 0 && nextTerm.compareTo(currentTerm) < 0) {
						nextTerm = currentTerm;
					} else {
						continue;
					}

				}
				this.seeklist.put(term, this.raIndexFile.getFilePointer());
				this.bo.write(line.getBytes());
				this.bo.write(".".getBytes());
				line = "";
				if (term.equals(nextTerm)) {
					if (fileBeginnings[winnerSlot] == null) {
						nextTerm = getLowest(terms);
					} else {
						String currentLine = fileBeginnings[winnerSlot].readLine(); 
						if (currentLine == null) {
							fileBeginnings[winnerSlot].close();
							fileBeginnings[winnerSlot] = null;
							lines[winnerSlot] = null;
							terms[winnerSlot] = null;
							countDown--;
							nextTerm = getLowest(terms);
						} else {
							terms[winnerSlot] = new String(DatatypeConverter.parseBase64Binary(currentLine.substring(0, currentLine.indexOf(":"))));
							lines[winnerSlot] = currentLine.substring(currentLine.indexOf(":"));
							nextTerm = terms[winnerSlot];
						}
					}
					winnerSlot = -1;
				}
				term = nextTerm;
			}
			this.bo.close();	
			this.fos.close();
			this.raIndexFile.close();

			/*
			 * merge link index files
			 * 
			 * TODO: implement; I am just writing the whole list here
			 */

			try {
				File linkIndexFile = new File(this.dir
						+ IndexHandler.linkIndexFileName
						+ IndexHandler.fileExtension);
				this.raIndexFile = new RandomAccessFile(linkIndexFile, "rw");
				this.fos = new FileOutputStream(raIndexFile.getFD());
				this.bo = new BufferedOutputStream(fos);

				for (String key : this.getLinkIndex().getTitleLists().keySet()) {
					this.getLinkIndex().getTitleLists().get(key).toIndexString(bo);
				}

				raIndexFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			/*
			 * write the seeklist to a file - would be too big to stringify first
			 */
			this.seekListToFile(this.dir 
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

			/*
			 * write the title-id-mapping to a file
			 */
			writeStringifiedToFile(this.titlesToIdsToString(), this.dir 
					+ IndexHandler.titlesToIdsFileName 
					+ IndexHandler.fileExtension);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// find the lexicographically lowest in a collection of Strings
	private String getLowest(String[] lines) {
		String lowest = "";
		for (String line : lines) {
			if(line != null) {
				lowest = line;
				break;
			}
		}
		for(String line : lines) {
			if(line != null && lowest.compareTo(line) > 0)
				lowest = line;
		}
		return lowest;
	}

	private void writeStringifiedToFile(String content, String filename) throws IOException {
		FileWriter fos = new FileWriter(filename);
		BufferedWriter bo = new BufferedWriter(fos, 8192);
		bo.write(content);

		bo.close();
		fos.close();
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
	private String seekListToFile(String filename) {
		try {
			FileWriter fos = new FileWriter(filename);
			BufferedWriter bo = new BufferedWriter(fos, 8192);
			for (String term : this.seeklist.keySet()) {	// uses iterator
				bo.write(term);
				bo.write('\t');
				bo.write(this.seeklist.get(term) + "");
				bo.write('\t');
			}
			bo.close();
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "";
		//			return result.toString();
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

		for (Long id : this.idsToTitles.keySet()) {	// uses iterator
			result
			.append(id)
			.append('\t')
			.append(this.idsToTitles.get(id))
			.append('\t');
		}
		// remove the last '\t'
		result.deleteCharAt(result.lastIndexOf("\t"));

		return result.toString();
	}

	private String titlesToIdsToString() {
		StringBuilder result = new StringBuilder();

		for (String title : this.getTitlesToIds().keySet()) {
			result
			.append(title)
			.append('\t')
			.append(this.getTitlesToIds().get(title))
			.append('\t');
		}
		// remove the last '\t'
		result.deleteCharAt(result.lastIndexOf("\t"));

		return result.toString();
	}

	/**
	 * Load existing seek list, id-title-mapping and title-id-mapping. 
	 * If the necessary files are not present in the directory, log a 
	 * message but proceed. If an IOException occurs, print it, but proceed.
	 */
	private void loadIndex() {
		try {
			// load the seek list
			File seekListFile = new File(this.dir 
					+ IndexHandler.seekListFileName 
					+ IndexHandler.fileExtension);

			Scanner scanner = new Scanner(seekListFile);
			scanner.useDelimiter("\\A");
			this.parseSeekListFileString(scanner.next());
			scanner.close();

			// load the seek list of the texts file
			File textsSeekListFile = new File(this.dir 
					+ IndexHandler.textsSeekListFileName 
					+ IndexHandler.fileExtension);

			scanner = new Scanner(textsSeekListFile);
			scanner.useDelimiter("\\A");
			this.parseTextsSeekListFileString(scanner.next());
			scanner.close();

			// load the id-titles-mapping
			File titlesFile = new File(this.dir 
					+ IndexHandler.titlesFileName 
					+ IndexHandler.fileExtension);
			scanner = new Scanner(titlesFile);
			scanner.useDelimiter("\\A");
			this.parseTitlesFileString(scanner.next());
			scanner.close();

			// load the titles-id-mapping
			File titlesToIdsFile = new File(this.dir 
					+ IndexHandler.titlesToIdsFileName
					+ IndexHandler.fileExtension);
			scanner = new Scanner(titlesToIdsFile);
			scanner.useDelimiter("\\A");
			this.parseTitlesToIdsFileString(scanner.next());
			scanner.close();
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
		Scanner tok = new Scanner(string);
		tok.useDelimiter("\t");
		boolean isTerm = true;	// whether the current token is the term
		String term = null;
		Long offset = null;

		while (tok.hasNext()) {
			String token = tok.next();
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
		tok.close();
	}

	/**
	 * Construct a seek list of the texts file from the read string. 
	 * If an exception occurs, print it, but proceed.
	 * @param string seek list file string
	 */
	private void parseTextsSeekListFileString(String string) {
		Scanner tok = new Scanner(string);
		tok.useDelimiter("\t");
		boolean isId = true;	// whether the current token is the id
		Long id = null;
		Long offset = null;

		while (tok.hasNext()) {
			String token = tok.next();
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
		tok.close();
	}

	/**
	 * Construct an id-titles-mapping from the read string. If an exception occurs,
	 * print it, but proceed.
	 * @param string seek list file string
	 */
	private void parseTitlesFileString(String string) {
		Scanner tok = new Scanner(string);
		tok.useDelimiter("\t");
		boolean isId = true;	// whether the current token is the id
		Long id = null;
		String title = null;
		while (tok.hasNext()) {
			String token = tok.next();
			try {
				if (isId) {
					// parse the term
					id = Long.parseLong(token);
				} else {
					// parse the term's offset and add it to the map
					title = token;
					this.idsToTitles.put(id, title);
				}
				isId = !isId;
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		}
		tok.close();
	}

	private void parseTitlesToIdsFileString(String string) {
		Scanner tok = new Scanner(string);
		tok.useDelimiter("\t");
		boolean isTitle = true;
		Long id = null;
		String title = null;
		while (tok.hasNext()) {
			String token = tok.next();
			try {
				if (isTitle) {
					title = token;
				} else {
					id = Long.parseLong(token);
					this.getTitlesToIds().put(title, id);
				}
				isTitle = !isTitle;
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		}
		tok.close();
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
					+ IndexHandler.linkIndexFileName 
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
			this.getErasedFile(dir 
					+ IndexHandler.titlesToIdsFileName
					+ IndexHandler.fileExtension);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Tests whether the given directory has all necessary index files
	 * (index file, seek list, id-title-mapping, ...). If an IOException 
	 * occurs, return <tt>false</tt>.
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
		File linkIndexFile = new File(dir 
				+ IndexHandler.linkIndexFileName
				+ IndexHandler.fileExtension);
		if (!linkIndexFile.canRead()) {
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
		File titlesToIdsFile = new File(dir 
				+ IndexHandler.titlesToIdsFileName 
				+ IndexHandler.fileExtension);
		if (!titlesToIdsFile.canRead()) {
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
				File indexFile = new File(this.dir + IndexHandler.indexFileName + IndexHandler.fileExtension);
				RandomAccessFile raIndexFile = new RandomAccessFile(indexFile, "r");

				/* 
				 * read the file until the list is finished (i.e., until the 
				 * first '.' or end of file
				 */
				raIndexFile.seek(offset);
				StringBuilder builder = new StringBuilder();
				try {
					while (true) {
						char character = (char)raIndexFile.read();
						builder.append(character);
						if (("" + character).equals(".")) {
							break;
						}
					}
				} catch (EOFException e) {
					// continue
				}
				raIndexFile.close();

				new Index.TermList();
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

	public LinkIndex.TitleList readListForTitle(String title) {
		if (title == null || "".equals(title)) {
			return null;
		}

		String processedTitle = LinkIndex.processTitle(title);

		try {
			File indexFile = new File(this.dir 
					+ IndexHandler.linkIndexFileName 
					+ IndexHandler.fileExtension);
			RandomAccessFile raIndexFile = new RandomAccessFile(indexFile, "r");

			// find line via binary search

			// number of characters in the file (assume: 2 bytes per character)
			long charNumber = raIndexFile.length() / 2;

			long offset = 0;
			long leftOffset = 0;
			long rightOffset = Math.max(0, (charNumber - 1) * 2);

			long maxTries = 1;
			while (charNumber > 0) {
				charNumber /= 2;
				maxTries++;
			}

			while (maxTries > 0) {
				// read the next line
				StringBuilder lineBuilder = new StringBuilder();
				while (true) {
					char nextChar = (char) raIndexFile.read();
					if (nextChar == '\n') {
						break;
					} else {
						lineBuilder.append(nextChar);
					}
				}
				// build a TitleList
				LinkIndex.TitleList list = LinkIndex.TitleList
						.createFromIndexString(lineBuilder.toString());
				// check the title
				String listTitle = list.title;
				if (processedTitle.equals(listTitle)) {
					// return list
					raIndexFile.close();
					return list;
				} else {
					// recalculate offset
					if (processedTitle.compareTo(listTitle) < 0) {
						// processedTitle < listTitle, go left
						rightOffset = offset;
						offset -= (offset - leftOffset) / 2;
						if (offset < 0) {
							break;
						}
					} else {
						// processedTitle > listTitle, go right
						leftOffset = offset;
						offset += (rightOffset - offset) / 2;
						if (offset > (raIndexFile.length() - 1)) {
							break;
						}
					}
				}
				// move the file pointer
				raIndexFile.seek(offset);
				// go to the next line
				while (((char) raIndexFile.read()) != '\n') {}
				// decrease tries
				maxTries--;
			}

			raIndexFile.close();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Helper function which returns the total number of documents in the corpus.
	 * @return the total number of documents
	 */
	public int totalNumberOfDocuments() {
		return this.idsToTitles.keySet().size();
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
		 * TODO: make sure that UTF-8 works
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

	/*
	 * Getters
	 */
	
	public Map<String, Long> getSeeklist() {
		return seeklist;
	}
	public void setSeeklist(Map<String, Long> seeklist) {
		this.seeklist = seeklist;
	}

	public Map<Long, String> getIdsToTitles() {
		return idsToTitles;
	}
	public void setIdsToTitles(Map<Long, String> idsToTitles) {
		this.idsToTitles = idsToTitles;
	}

	public LinkIndex getLinkIndex() {
		return linkIndex;
	}
	public void setLinkIndex(LinkIndex linkIndex) {
		this.linkIndex = linkIndex;
	}

	public Map<String, Long> getTitlesToIds() {
		return titlesToIds;
	}
	public void setTitlesToIds(Map<String, Long> titlesToIds) {
		this.titlesToIds = titlesToIds;
	}
	
	

}