package de.hpi.krestel.mySearchEngine;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;
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

import de.hpi.krestel.mySearchEngine.LinkIndex.TitleList;

/**
 * Handles everything related to the index. Deals with related I/O.
 * Gets id, title and text per article from the SAXHandler.
 * Merges everything into the final index file once the SAXHandler has
 * finished parsing.
 * (in the future:) Provides information for the query engine.
 */
class IndexHandler {

	// just to provide a simple way to switch between full index creation and just merging
	public static final boolean DEV_MODE = false;

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
	private static final String germanStopWordsFileName = "/GermanStopWords.csv";

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

	private static int THRESHOLD = 128 * 1024 * 1024;
//	private static int THRESHOLD = 160 * 64;
	private static int bufferSize = 8192;
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
	private File linkIndexFile;
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

		this.seeklist = new HashMap<String, Long>();
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

		InputStreamReader r = new InputStreamReader(this.getClass().getResourceAsStream(IndexHandler.germanStopWordsFileName));
		BufferedReader buffRead = new BufferedReader(r);
		String stopWordLine = "";
		try {
			stopWordLine = buffRead.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		HashSet<String> stopWords = new HashSet<String>();
		StringTokenizer st = new StringTokenizer(stopWordLine, ",");
        while (st.hasMoreTokens())
            stopWords.add(st.nextToken());
		
		/* 
		 * Create and return the analyzer with the given compatibility version.
		 * As of version 3.1, Snowball stopwords are used per default (can
		 * be accessed via GermanAnalyzer.getDefaultStopSet());
		 * 
		 * so we might replace these characters (or all UTF-8 characters which
		 * are not ASCII in general) with some other representation.
		 */
		Analyzer analyzer = new GermanAnalyzer(version, CharArraySet.copy(version, stopWords));

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
			raTextsFile.write(cleanPageText(text).getBytes(Charset.forName("UTF-8")));

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
		
		this.linkIndexFile = new File(this.dir
				+ IndexHandler.linkIndexFileName
				+ "_"
				+ this.fileCount
				+ IndexHandler.tempFileExtension);
		try {
			/*
			 * write part of index
			 */				
			this.raIndexFile = new RandomAccessFile(indexFile, "rw");
			this.fos = new FileOutputStream(raIndexFile.getFD());
			this.bo = new BufferedOutputStream(fos, IndexHandler.bufferSize);

			// get map of terms and their occurrence lists
			Map<String, Index.TermList> termLists = this.index.getTermLists();
			// write each occurrence list to the file
			for (String term : termLists.keySet()) {	// uses iterator
				// write the list using custom toIndexString method of TermList
				termLists.get(term).toIndexString(this.bo, term, true);
			}
			this.bo.close();
			this.fos.close();

			/*
			 * write part of link index
			 * 
			 */
			this.raIndexFile = new RandomAccessFile(this.linkIndexFile, "rw");
			this.fos = new FileOutputStream(this.raIndexFile.getFD());
			this.bo = new BufferedOutputStream(this.fos, IndexHandler.bufferSize);

			// get map of terms and their occurrence lists
			Map<String, TitleList> titleLists = this.linkIndex.getTitleLists();
			// write each occurrence list to the file
			for (String title : titleLists.keySet()) {	// uses iterator
				// write the list using custom toIndexString method of TermList
				titleLists.get(title).toIndexString(this.bo);
			}
			this.bo.close();
			this.fos.close();

			this.fileCount++;
		} catch(IOException e) {
			e.printStackTrace();
		}

		this.index = new Index();
		this.linkIndex = new LinkIndex();
	}

	/**
	 * Merges all parts of the index.
	 * Merges all parts of the link index.
	 * Creates the seeklist.
	 * Writes the index, the seeklist, the id-titles-mapping and the 
	 * titles-id-mapping to files (one file each).
	 * If an IOException occurs, print it, but proceed.
	 */
	public void createIndex() {
		try {
			/*
			 * write remaining parts of the index and link index
			 */
			writeToIndexFile();
			
			/*
			 * write the seeklist of the texts file to a file
			 */
//			writeStringifiedToFile(this.textsSeekListToString(), this.dir
//					+ IndexHandler.textsSeekListFileName
//					+ IndexHandler.fileExtension);
//			this.textsSeeklist = null;
//
//			/*
//			 * write the id-title-mapping to a file
//			 */
//			writeStringifiedToFile(this.titlesToString(), this.dir 
//					+ IndexHandler.titlesFileName 
//					+ IndexHandler.fileExtension);
//			this.idsToTitles = null;
//			
//			/*
//			 * write the title-id-mapping to a file
//			 */
//			writeStringifiedToFile(this.titlesToIdsToString(), this.dir 
//					+ IndexHandler.titlesToIdsFileName 
//					+ IndexHandler.fileExtension);
//			this.titlesToIds = null;
//
//			/*
//			 * merge link index files
//			 */
//			mergeTempFilesIntoFile(IndexHandler.linkIndexFileName, false);
//			this.linkIndex = null;
			
			/*
			 * merge index files
			 */
			mergeTempFilesIntoFile(IndexHandler.indexFileName, true);

//			deleteTemporaryFiles();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void deleteTemporaryFiles() {
		FilenameFilter filter = new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(IndexHandler.tempFileExtension);
			}
		};

		File directory = new File(this.dir);
		File[] filesInFolder = directory.listFiles(filter);
		for (File file : filesInFolder) file.delete();
	}

	private void mergeTempFilesIntoFile(final String fileName, boolean base64Encoded) throws IOException {
		FilenameFilter filter = new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.startsWith(fileName) && name.endsWith(IndexHandler.tempFileExtension);
			}
		};

		File directory = new File(this.dir);
		File leFile = new File(this.dir
				+ fileName
				+ IndexHandler.fileExtension);
		this.raIndexFile = new RandomAccessFile(leFile, "rw");
		this.fos = new FileOutputStream(this.raIndexFile.getFD());
		this.bo = new BufferedOutputStream(this.fos, IndexHandler.bufferSize);
		File[] filesInFolder = directory.listFiles(filter);
		int fileCount = filesInFolder.length;
		
		OutputStream seeklistFile = null;
		OutputStream foss = null;
		ObjectOutput slBo = null;
		if (fileName.equals(IndexHandler.indexFileName)) {
			File le = new File(this.dir
					+ IndexHandler.seekListFileName
					+ ".ser");
			seeklistFile = new FileOutputStream(le);
			foss = new BufferedOutputStream(seeklistFile);
			slBo = new ObjectOutputStream(foss);
		}
		BufferedReader[] fileBeginnings = new BufferedReader[fileCount];
		
		String[] terms = new String[fileCount];
		String[] lines = new String[fileCount];

		String[] lineBuffer = new String[fileCount];
		
		setupMergingToolsForTempFiles(filesInFolder, fileBeginnings, terms, lines, lineBuffer, base64Encoded);
		
		String term = getLowest(terms);
		String nextTerm = term;
		int index = 0;
		int countDown = fileCount;
		int winnerSlot = -1;
		boolean firstLine = true;
		String currentLine = "";
		int termIndex = -1;
		/*
		 * whenever a term is merged the next line from the file it originated from is read
		 * this is continued until all lines in all files are read / all readers reached the end of the file
		 * lines of files will be merged whenever a read line yields the same term  
		 */
		int safetyCounter = countDown * 2;
		while(countDown > 0 || safetyCounter > 0) {
			safetyCounter--;
			for(index = 0; index < fileCount; index++) {
				if (lines[index] == null) continue;
				String currentTerm = terms[index];
				if (term.compareTo(currentTerm) == 0) {
					if (!firstLine)
						bo.write(";".getBytes());
					if (firstLine && fileName.equals(IndexHandler.indexFileName)) {
						this.bo.flush(); this.fos.flush();
//						slBo.write(term.getBytes());
//						slBo.write("\t".getBytes());
//						slBo.write((this.raIndexFile.getFilePointer() + "").getBytes());
//						slBo.write("\n".getBytes());
						this.seeklist.put(term, this.raIndexFile.getFilePointer());
					};
					if (firstLine && fileName.equals(IndexHandler.linkIndexFileName)) {
						this.bo.write(term.getBytes());
						this.bo.write(TitleList.colon);
					};
					bo.write(lines[index].substring(1, lines[index].lastIndexOf(".")).getBytes());
					firstLine = false;
					currentLine = fileBeginnings[index].readLine();
					if (currentLine == null || currentLine.trim().isEmpty()) {
						fileBeginnings[index].close();
						fileBeginnings[index] = null;
						terms[index] = null;
						lines[index] = null;
						countDown--;
						if (fileName.equals(IndexHandler.indexFileName)) {
							term = getLowest(lines);
							if (!term.isEmpty())
								term = term.substring(0, term.indexOf(":"));
							else continue;
						} else {
							term = getLowest(terms);
							if (term.isEmpty()) term = nextTerm;
						}
						
					} else {
						currentLine = currentLine.trim(); 
						lineBuffer[index] = currentLine;
						terms[index] = conditionalBase64Converter(currentLine, base64Encoded).trim();
						if (currentLine.contains(":"))
							lines[index] = currentLine.substring(currentLine.indexOf(":")).trim();
						else lines[index] = ":###.";
						winnerSlot = index;
					}
				} else if (term.compareTo(currentTerm) < 0 && nextTerm.compareTo(currentTerm) < 0) {
					nextTerm = getLowest(terms);
				} else {
					continue;
				}
			}
			if (term.isEmpty()) continue;
			if (!firstLine)
				this.bo.write(TitleList.dot);
			if (!firstLine && fileName.equals(IndexHandler.linkIndexFileName)) this.bo.write("\n".getBytes());
			if (term.equals(nextTerm)) {
				if (winnerSlot == -1) break;
				if (fileBeginnings[winnerSlot] != null) {
					currentLine = fileBeginnings[winnerSlot].readLine();
					if (currentLine == null || currentLine.trim().isEmpty()) {
						fileBeginnings[winnerSlot].close();
						fileBeginnings[winnerSlot] = null;
						lines[winnerSlot] = null;
						terms[winnerSlot] = null;
						countDown--;
					} else {
						currentLine = currentLine.trim();
						lineBuffer[winnerSlot] = currentLine;
						terms[winnerSlot] = conditionalBase64Converter(currentLine, base64Encoded).trim();
						lines[winnerSlot] = currentLine.substring(currentLine.indexOf(":")).trim();
					}
					currentLine = null;
				}
				nextTerm = getLowest(terms);
				winnerSlot = -1;
			} else {
				if (firstLine && fileName.equals(IndexHandler.indexFileName)) {
					this.bo.flush(); this.fos.flush();
//					slBo.write(term.getBytes());
//					slBo.write("\t".getBytes());
//					slBo.write((this.raIndexFile.getFilePointer() + "").getBytes());
//					slBo.write("\n".getBytes());
					this.seeklist.put(term, this.raIndexFile.getFilePointer());
				}; 
				if (firstLine && fileName.equals(IndexHandler.linkIndexFileName)) {
					this.bo.write(term.getBytes());
					this.bo.write(TitleList.colon);
				};
				if (termIndex == -1) termIndex = getLowestIndex(lineBuffer, term, base64Encoded);
				if (termIndex == -1) {
					term = nextTerm;
					firstLine = true;
					if (!firstLine && fileName.equals(IndexHandler.linkIndexFileName)) this.bo.write("\n".getBytes());
					continue;
				}
			}
			term = nextTerm;
			firstLine = true;
			this.bo.flush();
			this.fos.flush();
		}
		this.bo.flush();
		this.fos.flush();
		this.bo.close();	
		this.fos.close();
		this.raIndexFile.close();
		if (fileName.equals(IndexHandler.indexFileName)) {
			slBo.writeObject(this.seeklist);
			slBo.flush();
			foss.flush();
			slBo.close();
			foss.close();
			seeklistFile.close();
		}
	}

	private void setupMergingToolsForTempFiles(File[] filesInFolder, BufferedReader[] fileBeginnings, String[] terms, String[] lines, String[] lineBuffer, boolean base64Encoded) throws FileNotFoundException, IOException {
		int index = 0;
		String line = "";
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
			lineBuffer[index] = line;
			if (line == null || line.trim().isEmpty()) {
				breed.close();
				fileBeginnings[index] = null;
			} else {
				terms[index] = conditionalBase64Converter(line, base64Encoded);
				lines[index] = line.substring(line.indexOf(":"));
			}
			index++;
		}
	}
	
	private String conditionalBase64Converter(String content, boolean conversionRequired) {
		String interestingPart = "";
		if (content.contains("."))
			interestingPart = content.substring(0, content.indexOf(":"));
		else interestingPart = content;
		content = null;
		return conversionRequired ? new String(DatatypeConverter.parseBase64Binary(interestingPart)) : interestingPart;
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
			if(line != null && !line.isEmpty() && lowest.compareTo(line.trim()) > 0)
				lowest = line;
		}
		lines = null;
		return lowest.trim();
	}
	
	private int getLowestIndex(String[] lines, String lowest, boolean b64Required) {
		int index = 0;
		for(String line : lines) {
			if (line == null) continue;
			if (b64Required)
				line = new String(DatatypeConverter.parseBase64Binary(line.substring(0, line.indexOf(":"))));
			if(line != null && line.trim().startsWith(lowest))
				return index;
			index++;
		}
		return -1;
	}

	private void writeStringifiedToFile(String content, String filename) throws IOException {
		FileWriter fos = new FileWriter(filename);
		BufferedWriter bo = new BufferedWriter(fos, IndexHandler.bufferSize);
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
			BufferedWriter bo = new BufferedWriter(fos, IndexHandler.bufferSize);
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
			String firstPart = "";
			// load the seek list of the texts file
			File textsSeekListFile = new File(this.dir 
					+ IndexHandler.textsSeekListFileName 
					+ IndexHandler.fileExtension);

			Scanner scanner = new Scanner(textsSeekListFile);
			scanner.useDelimiter("\t");
			while (scanner.hasNext())
				if (!firstPart.isEmpty()) {
					this.parseTextsSeekListFileString(firstPart + "\t" + scanner.next());
					firstPart = "";
				} else {
					firstPart = scanner.next();
				}
			scanner.close();

			// load the id-titles-mapping
			File titlesFile = new File(this.dir 
					+ IndexHandler.titlesFileName 
					+ IndexHandler.fileExtension);
			scanner = new Scanner(titlesFile);
			scanner.useDelimiter("\t");
			while (scanner.hasNext())
				if (!firstPart.isEmpty()) {
					this.parseTitlesFileString(firstPart + "\t" + scanner.next());
					firstPart = "";
				} else {
					firstPart = scanner.next();
				}
			scanner.close();

			// load the titles-id-mapping
			File titlesToIdsFile = new File(this.dir 
					+ IndexHandler.titlesToIdsFileName
					+ IndexHandler.fileExtension);
			scanner = new Scanner(titlesToIdsFile);
			scanner.useDelimiter("\t");
			while (scanner.hasNext())
				if (!firstPart.isEmpty()) {
					this.parseTitlesToIdsFileString(firstPart + "\t" + scanner.next());
					firstPart = "";
				} else {
					firstPart = scanner.next();
				}

			scanner.close();

			// load the seek list
			InputStream seekListFile = new FileInputStream(this.dir 
					+ IndexHandler.seekListFileName 
					+ ".ser");
			InputStream reader = new BufferedInputStream(seekListFile);
			ObjectInput bread = new ObjectInputStream(reader);
	        try {
				this.seeklist = (Map<String, Long>)bread.readObject();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// set a small buffersize to avoid unnecessary copies of the containing array
//			String line = "";
//			String[] parts = null;
//			while ((line = bread.readLine()) != null) {	
//				parts = line.split("\t");
//				this.seeklist.put(parts[0], Long.parseLong(parts[1]));
//				System.out.println(Runtime.getRuntime().freeMemory() / 1024 / 1024 + 
//					" of total: " + Runtime.getRuntime().totalMemory() / 1024 / 1024);
//			}
			bread.close();
			seekListFile.close();
			System.out.println("seeklist complete");
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
		String[] parts = string.split("\t");
		this.seeklist.put(parts[0], Long.parseLong(parts[1]));
	}

	/**
	 * Construct a seek list of the texts file from the read string. 
	 * If an exception occurs, print it, but proceed.
	 * @param string seek list file string
	 */
	private void parseTextsSeekListFileString(String string) {
		String[] parts = string.split("\t");
		this.textsSeeklist.put(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
	}

	/**
	 * Construct an id-titles-mapping from the read string. If an exception occurs,
	 * print it, but proceed.
	 * @param string seek list file string
	 */
	private void parseTitlesFileString(String string) {
		String[] parts = string.split("\t");
		this.idsToTitles.put(Long.parseLong(parts[0]), parts[1]);
	}

	private void parseTitlesToIdsFileString(String string) {
		String[] parts = string.split("\t");
		this.getTitlesToIds().put(parts[0], Long.parseLong(parts[1]));
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
	 * term and retrieve the text around that occurrence, or use the beginning
	 * of the document if no such occurrence is available.
	 * @param documentId the ID of the document
	 * @param queryTerms the relevant terms of the query; may be <tt>null</tt>
	 *   or empty, in which case it is ignored
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
		String text = null;
		try {
			// get the file
			File textsFile = new File(this.dir + IndexHandler.textsFileName + IndexHandler.fileExtension);
			RandomAccessFile raTextsFile = new RandomAccessFile(textsFile, "r");

			/* 
			 * read the text (or its beginning, which is enough for the purpose
			 * of making a snippet)
			 */
			raTextsFile.seek(offset);
			
			long maxLength = raTextsFile.length() - offset;
			int textSize = 10000 <= maxLength ? 10000 : ((int) maxLength);
			
			byte[] textBytes = new byte[textSize];
			// read the bytes
			raTextsFile.readFully(textBytes);
			// create a string from the bytes
			text = new String(textBytes, "UTF-8");
			// limit the string, if a delimiter is found
			int delimiterIndex = text.indexOf("\t");
			if (delimiterIndex != -1) {
				text = text.substring(0, delimiterIndex);
			}
			
			raTextsFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (text == null || text.equals("")) {
			// no text
			return null;
		}
		
		// search for an occurrence of a query term
		Integer termOccurrenceIndex = null;
		if (queryTerms != null && !queryTerms.isEmpty()) {
			String[] termCandidates = text.split(" ", 500);	// limit to first 500 terms for performance
			for (String term : termCandidates) {
				String processedTerm = null;
				try {
					List<String> processedTermResult = this.processRawText(term);
					if (processedTermResult != null && !processedTermResult.isEmpty() 
							&& !processedTermResult.get(0).equals("")) {
						processedTerm = processedTermResult.get(0);
					}
				} catch (IOException e) {
					e.printStackTrace();  // should not happen
				}
				if (processedTerm != null && !"".equals(processedTerm)) {
					if (queryTerms.contains(processedTerm)) {
						termOccurrenceIndex = text.indexOf(term);
						break;
					}
				}
			}
		}

		/*
		 * Create a snippet from the text.
		 */
		int startIndex = 0;
		int endIndex = 0;
		if (termOccurrenceIndex == null) {
			// simple algorithm: just use the beginning
			endIndex = Math.min(240, text.length());
			int lastSpaceIndex = text.lastIndexOf(" ", (endIndex - 1));
			if (lastSpaceIndex > 200) {
				endIndex = lastSpaceIndex;
			}
			return text.substring(startIndex, endIndex)
					.replace('\n', ' ')			// remove newlines
					+ "...";
		} else {
			// advanced algorithm: use text around the occurrence
			if (termOccurrenceIndex > 100) {
				startIndex = text.lastIndexOf(" ", termOccurrenceIndex - 100) + 1;
				if (termOccurrenceIndex - startIndex > 140) {
					startIndex = termOccurrenceIndex - 100;
				}
			}
			endIndex = Math.min(termOccurrenceIndex + 120, text.length());
			int lastSpaceIndex = text.lastIndexOf(" ", (endIndex - 1));
			if (lastSpaceIndex - startIndex > 200) {
				endIndex = lastSpaceIndex;
			}
			return (startIndex > 0 ? "..." : "")
					+ text.substring(startIndex, endIndex).replace('\n', ' ')	// remove newlines
					+ "...";
		}
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