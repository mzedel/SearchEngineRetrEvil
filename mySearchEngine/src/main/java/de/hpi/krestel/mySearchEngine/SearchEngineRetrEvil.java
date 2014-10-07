package de.hpi.krestel.mySearchEngine;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.InputSource;
import org.xml.sax.helpers.DefaultHandler;

/*
 * TODO: ggf. keyword queries als phrase queries behandeln und Resultate bevorzugen
 * 
 * SearchEngineRetrEvil by Manuel Zedel and Tim Sporleder.
 * 
 * Indexing:
 *  - We use Lucene for pre-processing documents and queries (lower case,
 *    tokenizing, stemming, stopword removal using a newer german list)
 *  - Redirection pages are not indexed to begin with. Therefore, they are not
 *    considered in the ranking (of other documents) either.
 * Query processing:
 *  - Queries may be either link queries, boolean queries or keyword queries.
 *  - Link queries include the string "LINKTO ". Everything else in the query
 *    will be interpreted as the title of the respective page (no matter where
 *    the "LINKTO " is located in the query).
 *  - Boolean queries have at least one boolean operator which must be in upper 
 *    case and enclosed in whitespace (e.g. " AND ").
 *  - Keyword queries are all queries which are not link queries or boolean
 *    queries.
 *  - Both boolean queries and keyword queries may include phrase queries
 *    (with phrases enclosed like "phrase" or 'phrase') and prefix queries
 *    (with *). In keyword queries, phrase queries and prefix queries are
 *    extracted, executed as a boolean query, and the result set restricts the
 *    result set of the keyword query.
 * Snippets:
 *  - Snippets are created from the text of their respective page, which is
 *    stored while indexing.
 *  - Relevant query terms for snippet creation are extracted by removing all
 *    operators (boolean, LINKTO, *, quotation marks) from the query text and
 *    pre-processing the remaining query. If one of the resulting terms is found
 *    in the text of the page, the snippet is created from the surrounding
 *    lines (this may not work for link queries and prefix queries). Otherwise,
 *    the beginning of the page is used.
 */
public class SearchEngineRetrEvil extends SearchEngine {
	
	// treat boolean queries as keyword queries and ignore all boolean operators
	private final static boolean WEAK_BOOLEAN_MODE = true;
	
	/**
	 * Boolean operator "AND" in upper case
	 */
	private static final String AND = " AND ";
	/**
	 * Boolean operator "OR" in upper case
	 */
	private static final String OR = " OR ";
	/**
	 * Boolean operator "BUT NOT" in upper case
	 */
	private static final String BUT_NOT = " BUT NOT ";
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
	 * 
	 * Note: after due consideration, we chose to disregard normalization regarding
	 * document length and we do not collect any information about document length
	 * during indexing, so changing this parameter to something other than 0 is
	 * not wise.
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
	
	/**
	 * Initialize the engine. Do not change!
	 */
	public SearchEngineRetrEvil() {
		super();	
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
		// get dump file
		String dumpFile = new File(dir).getParent() + "/" + "deWikipediaDump.xml";
		if (IndexHandler.DEV_MODE)
			dumpFile = new File(dir).getParent() + "/" + "testDump.xml";
		/* 
		 * create the indexer with the target dir; this instance is only used for
		 * creating the index, not for answering queries
		 */
		IndexHandler indexer = new IndexHandler(dir);
		
		File fileDirectory = new File(dir);
		if (fileDirectory.listFiles().length > 10) 
			indexer.createIndex();
		try {
			// get the SAX parser with the appropriate handler
			SAXParser saxParser = SAXParserFactory.newInstance().newSAXParser();			
			DefaultHandler saxHandler = new SAXHandler(this, indexer);

			// parse the dump (UTF-8)
			InputStream inputStream = new FileInputStream(dumpFile);
			Reader reader = new InputStreamReader(inputStream,"UTF-8");
			InputSource source = new InputSource(reader);
			source.setEncoding("UTF-8");
			
			saxParser.parse(source, saxHandler);
		} catch (Exception e) {
			this.log("Exception during SAX parsing: " + e.toString());
			e.printStackTrace();
		}
	}

	/**
	 * Load the index.
	 * @param dir base directory of the index files
	 * @returns <tt>true</tt> if the index was loaded successfully;
	 *   <tt>false</tt> otherwise, especially if files are missing (i.e., the
	 *   index must be created first)
	 */
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

	/**
	 * Evaluate the given query. It is interpreted as either a link query,
	 * a boolean query or a keyword query.
	 * @param query the query text
	 * @param topK number of ranked documents to be returned (applies to
	 *   keyword queries only)
	 * @param prf the number of documents to be used for pseudo relevance 
	 *   feedback (0 means no pseudo relevance feedback is used, applies to 
	 *   keyword queries only)
	 */
	@SuppressWarnings("unused")
	@Override
	ArrayList<String> search(String query, int topK, int prf) {
		if (query == null || topK <= 0) {
			// invalid arguments: return an empty result set
			return new ArrayList<String>();
		}
		
		List<Long> documentIds;
		if (isLinkQuery(query)) {
			// a link query; do not extract query terms
			documentIds = processLinkQuery(query);
		} else if (!WEAK_BOOLEAN_MODE && isBooleanQuery(query)) {
			// a query which yields a binary ranking
			documentIds = processBooleanQuery(query);
		} else {
			// a query which yields a graded ranking
			documentIds = processKeywordQuery(query, topK, prf);
		}
		
		// return ranking with titles and snippets
		return this.createQueryAnswerForDocuments(documentIds, query);
	}
	
	/**
	 * Creates a list of String representations for the documents denoted by the
	 * given IDs.<br>
	 * For each document, the title followed by a snippet is returned.<br>
	 * If the given list is <tt>null</tt> or empty, an empty list is returned.
	 * @param documentIds the IDs of the relevant documents
	 * @param query the query text
	 * @return a list of String representations of the relevant documents which
	 * 	is never <tt>null</tt>
	 */
	private ArrayList<String> createQueryAnswerForDocuments(
			List<Long> documentIds, String query) {
		// catch unsuited arguments
		if (documentIds == null || documentIds.size() <= 0) {
			return new ArrayList<String>();
		}
		
		ArrayList<String> result = new ArrayList<String>(documentIds.size());
		List<String> queryTerms = null;
		if (query != null) {
			try {
				queryTerms = this.indexHandler.processRawText(removeAllOperators(query));
			} catch (IOException e) {
				e.printStackTrace();	// should not happen
			}
		}
		
		for (Long documentId : documentIds) {
			// get the title of the document
			String title = this.indexHandler.getIdsToTitles().get(documentId);
			
			// get a snippet of the document
			String snippet = this.indexHandler.getSnippetForDocumentId(documentId, queryTerms);
				
			// store: title + newline (unless snippet is null) + snippet
			result.add((title != null ? title : "") + (snippet != null ? ("\n" + snippet) : ""));
		}
		
		return result;
	}
	
	/**
	 * Extract the title from a query answer (see 
	 * {@link #createQueryAnswerForDocuments(List, List)}).
	 * @param queryAnswer the query answer
	 * @return the title, never <tt>null</tt>
	 */
	private String getTitleFromQueryAnswer(String queryAnswer) {
		if (queryAnswer == null) {
			return "";
		}
		if (queryAnswer.contains("\n")) {
			int titleEnd = queryAnswer.indexOf('\n');
			if (titleEnd > 0) {
				return queryAnswer.substring(0, titleEnd);
			} else {
				return "";
			}
		} else {
			return queryAnswer;
		}
	}
	
	/**
	 * Process the query as a prefix query, i.e., find documents containing
	 * any indexed term which starts with the given prefix.
	 * @param query the query text
	 * @return a list of document IDs
	 */
	private List<Long> processPrefixQuery(String query) {
		// extract the prefix
		String prefix = query.trim().substring(0, query.indexOf("*"));
		
		// pre-process prefix
		prefix = prefix.toLowerCase();

		// get all relevant terms
		List<String> terms = this.indexHandler.getTermsForPrefix(prefix);
		
		// get all relevant documents
		Set<Long> documentIds = new TreeSet<Long>();
		int countTerms = 0;
		for (String term : terms) {
			Index.TermList termList = this.indexHandler.readListForTerm(term, false);
			if (termList != null) {
				documentIds.addAll(termList.getOccurrences().keySet());
				if (++countTerms >= 10) {
					break;	// do not allow too many terms
				}
			}
		}
		
		// if nothing is found: do it again with thorough pre-processing
		if (documentIds.size() == 0) {
			try {
				List<String> processed = this.indexHandler.processRawText(prefix);
	
				// get all relevant terms
				terms = this.indexHandler.getTermsForPrefix(processed.get(0));
				
				// get all relevant documents
				documentIds = new TreeSet<Long>();
				countTerms = 0;
				for (String term : terms) {
					Index.TermList termList = this.indexHandler.readListForTerm(term, false);
					if (termList != null) {
						documentIds.addAll(termList.getOccurrences().keySet());
						if (++countTerms >= 10) {
							break;	// do not allow too many terms
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();	// should not happen
			}
		}
		
		return new ArrayList<Long>(documentIds);
	}
	
	/**
	 * Check if a given query is a boolean query (i.e., contains at least one
	 * boolean operator). Boolean operators must be in upper case.
	 * @param query the query text
	 * @return <tt>true</tt> if the query is a boolean query, 
	 * 		<tt>false</tt> otherwise
	 */
	private static boolean isBooleanQuery(String query) {
		for (String operator : SearchEngineRetrEvil.BOOLEAN_OPERATORS) {
			if (query.contains(operator)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Check if a given query is a link query (i.e., queries references to a
	 * given page).
	 * @param query the query text
	 * @return <tt>true</tt> if the query is a link query, 
	 * 		<tt>false</tt> otherwise
	 */
	private static boolean isLinkQuery(String query) {
		return query.contains("LINKTO ");
	}

	/**
	 * Check if a given query is a prefix query (i.e., queries for pages with
	 * terms starting with the given prefix).
	 * @param query the query text
	 * @return <tt>true</tt> if the query is a prefix query, 
	 * 		<tt>false</tt> otherwise
	 */
	private static boolean isPrefixQuery(String query) {
		return query.contains("*");
	}
	
	/**
	 * Check if a given query is a phrase query (i.e., queries for pages which
	 * include the exact phrase).
	 * @param query the query text
	 * @return <tt>true</tt> if the query is a phrase query, 
	 * 		<tt>false</tt> otherwise
	 */
	private static boolean isPhraseQuery(String query) {
		// check if the query includes a delimiter twice (to mark a phrase)
		int indexLeft, indexRight;
		
		indexLeft = query.indexOf('\'');
		if (indexLeft >= 0) {
			indexRight = query.indexOf('\'', indexLeft + 1);
			if (indexRight != -1) {
				return true; // 'phrase'
			}
		}
		indexLeft = query.indexOf('\"');
		if (indexLeft >= 0) {
			indexRight = query.indexOf('\"', indexLeft + 1);
			if (indexRight != -1) {
				return true; // "phrase"
			}
		}
		
		return false; // nothing found
	}
	
	/**
	 * Given a phrase query, extract its (first) phrase from it without the
	 * enclosing quotation marks (see {@link #isPhraseQuery(String)}).
	 * @param query the query text
	 * @return the phrase (may be "")
	 */
	private static String extractPhraseFromQuery(String query) {
		int indexLeft, indexRight;
		
		indexLeft = query.indexOf('\'');
		if (indexLeft >= 0) {
			indexRight = query.indexOf('\'', indexLeft + 1);
			if (indexRight != -1) {
				return query.substring(indexLeft + 1, indexRight); // 'phrase'
			}
		}
		indexLeft = query.indexOf('\"');
		if (indexLeft >= 0) {
			indexRight = query.indexOf('\"', indexLeft + 1);
			if (indexRight != -1) {
				return query.substring(indexLeft + 1, indexRight); // "phrase"
			}
		}
		
		return ""; // empty or missing phrase
	}
	
	/**
	 * Process the query as a boolean query.
	 * The operators are evaluated from left to right with no other precedence.
	 * @param query the query text
	 * @return a list of document IDs
	 */
	private List<Long> processBooleanQuery(String query) {
		// recursively divide the query into subqueries
		if (isBooleanQuery(query)) {
			// find the rightmost operator
			int indexAnd = query.lastIndexOf(AND);
			int indexOr = query.lastIndexOf(OR);
			int indexButNot = query.lastIndexOf(BUT_NOT);
			
			if (indexAnd != -1 
					&& (indexOr == -1 || indexAnd > indexOr) 
					&& (indexButNot == -1 || indexAnd > indexButNot)) {
				// intersection
				List<Long> leftSet = this.processBooleanQuery(query.substring(0, indexAnd));
				List<Long> rightSet = this.processBooleanQuery(query.substring(indexAnd + AND.length()));
				
				leftSet.retainAll(rightSet);
				return new ArrayList<Long>(new HashSet<Long>(leftSet));	// remove duplicates
			} else if (indexOr != -1 
					&& (indexAnd == -1 || indexOr > indexAnd) 
					&& (indexButNot == -1 || indexOr > indexButNot)) {
				// union
				List<Long> leftSet = this.processBooleanQuery(query.substring(0, indexOr));
				List<Long> rightSet = this.processBooleanQuery(query.substring(indexOr + OR.length()));
				
				leftSet.addAll(rightSet);
				return new ArrayList<Long>(new HashSet<Long>(leftSet));	// remove duplicates
			} else if (indexButNot != -1 
					&& (indexOr == -1 || indexButNot > indexOr) 
					&& (indexAnd == -1 || indexButNot > indexAnd)) {
				// difference
				List<Long> leftSet = this.processBooleanQuery(query.substring(0, indexButNot));
				List<Long> rightSet = this.processBooleanQuery(query.substring(indexButNot + BUT_NOT.length()));
				
				leftSet.removeAll(rightSet);
				return new ArrayList<Long>(new HashSet<Long>(leftSet));	// remove duplicates
			}
		} else if (isPrefixQuery(query)) {
			// prefix query
			return this.processPrefixQuery(query);
		} else if (isPhraseQuery(query)) {
			// phrase query
			return this.processPhraseQuery(query);
		} else {
			// single term
			try {
				List<String> terms = this.indexHandler.processRawText(query);
				if (terms.size() > 0) {
					Index.TermList termList = this.indexHandler.readListForTerm(terms.get(0), false);
					if (termList != null) {
						return new ArrayList<Long>(termList.getOccurrences().keySet());
					}
				}
			} catch (IOException e) {
				// should not happen
				e.printStackTrace();
			}
		}
		return new ArrayList<Long>();
	}
	
	/**
	 * Process the query as a link query.
	 * @param query the query text
	 * @return a list of document IDs
	 */
	private List<Long> processLinkQuery(String query) {
		// extract the target title
		String processedTitle = LinkIndex.processTitle(query.replace("LINKTO ", "").trim());
		// try to read the TitleList (may be null)
		LinkIndex.TitleList titleList = this.indexHandler
				.readListForTitle(processedTitle);
		
		// get the IDs of documents linking to the title
		List<Long> documentIds = new ArrayList<Long>();
		if (titleList != null) {
			// sort titles (TreeSet is inherently sorted by natural ordering)
			TreeSet<String> sortedTitles = new TreeSet<String>(titleList.getTitles());
			
			for (String listedTitle : sortedTitles) {
				// listedTitle is the pre-processed title, need the document ID
				Long documentId = this.indexHandler.getTitlesToIds().get(listedTitle);
				if (documentId != null && !documentIds.contains(documentId)) {
					documentIds.add(documentId);
				}
			}
		}
		
		return documentIds;
	}
	
	/**
	 * Process the query as a phrase query. If the phrase is empty (or missing),
	 * an empty list of documents is returned.
	 * @param query the query text
	 * @return a list of document IDs
	 */
	private List<Long> processPhraseQuery(String query) {
		// extract the phrase
		String phrase = extractPhraseFromQuery(query);
		if ("".equals(phrase)) {
			return new ArrayList<Long>();
		}
		
		// pre-process the phrase
		List<String> processedPhrase;
		try {
			processedPhrase = this.indexHandler.processRawText(phrase);
		} catch (IOException e) {	// should never happen
			processedPhrase = new ArrayList<String>();
			e.printStackTrace();
		}
		if (processedPhrase.size() == 0) {
			return new ArrayList<Long>();
		}
		
		// search for the given sequence of processed terms in documents
		List<Index.TermList> termLists = new ArrayList<Index.TermList>();
		for (String term : processedPhrase) {
			Index.TermList termList = this.indexHandler.readListForTerm(term, true);
			if (termList == null) {
				// term exists, but fetching was aborted => do not consider
				continue;
			}
			termLists.add(termList);
		}
		Set<Long> documentIds = new TreeSet<Long>();
		for (Entry<Long, Collection<Integer>> entry : termLists.get(0).getOccurrences().entrySet()) {
			long documentId = entry.getKey();
			startPositionsLoop: for (int startPosition : entry.getValue()) {
				int nextTermIndex = 1;
				int nextPosition = startPosition + 1;
				while (nextTermIndex < termLists.size()) {
					Collection<Integer> positions = termLists.get(nextTermIndex).getOccurrences().get(documentId);
					if (positions == null || !positions.contains(nextPosition)) {
						continue startPositionsLoop;
					}
					nextTermIndex++;
				}
				documentIds.add(documentId);
			}
		}
		
		return new ArrayList<Long>(documentIds);
	}
	
	/**
	 * Extracts boolean (i.e., set-based) parts of a (otherwise keyword-based)
	 * query and constructs a boolean query based on these parts.
	 * @param query the query text
	 * @param builder a builder for the boolean query, changed in place
	 * @return the keyword query without the extracted parts
	 */
	private static String extractBooleanQueryParts(String query, StringBuilder builder) {
		// extract components which are phrase / prefix / BUT_NOT queries
		while (isPhraseQuery(query)) {
			// extract leftmost phrase
			String phrase = extractPhraseFromQuery(query);
			builder.append(AND + "\"" + phrase.trim() + "\"");
			query = query.replaceAll("\"" + phrase + "\"", "");
			query = query.replaceAll("\'" + phrase + "\'", "");
		}
		while (isPrefixQuery(query)) {
			// extract leftmost prefix
			int endIndex = query.indexOf("*");
			if (endIndex != -1) {
				int startIndex = query.lastIndexOf(" ", endIndex);
				startIndex = startIndex != -1 ? startIndex : 0;
				String prefix = query.substring(startIndex, endIndex);
				builder.append(AND + prefix.trim() + "*");
				query = query.substring(0, startIndex).concat(query.substring(endIndex + 1));
			}
		}
		if (builder.length() >= AND.length()) {
			// remove starting AND
			builder.delete(0, AND.length());
		}
		
		return query.trim();
	}
	
	/**
	 * Remove all boolean operators from a query.
	 * @param query the query text
	 * @return the cleaned query text
	 */
	private static String removeBooleanOperators(String query) {
		for (String operator : BOOLEAN_OPERATORS) {
			query = query.replaceAll(operator, " ");
		}
		return query;
	}
	
	/**
	 * Remove all operators (boolean, LINKTO, *, quotation marks) from a query
	 * string. Used for snippet creation.
	 * @param query the query text
	 * @return the cleaned query text
	 */
	private static String removeAllOperators(String query) {
		return removeBooleanOperators(query)
				.replaceAll("LINKTO ", "")
				.replaceAll("[*]", "")
				.replaceAll("\"", "")
				.replaceAll("'", "");
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
	 * @return a list of document IDs
	 */
	private List<Long> processKeywordQuery(String query, int topK, int prf) {
		List<Long> result = new ArrayList<Long>();
		
		try {
			/*
			 * Remove boolean operators (if any), pre-process the query to get
			 * the query terms, and extract phrase queries
			 * and prefix queries which limit the result set.
			 */
			query = removeBooleanOperators(query);
			
			List<String> terms = this.indexHandler.processRawText(query);
			
			StringBuilder booleanQueryBuilder = new StringBuilder(100);
			query = extractBooleanQueryParts(query, booleanQueryBuilder);
			String booleanQuery = booleanQueryBuilder.toString();
			
			List<Long> potentialDocumentIds = null;
			if (booleanQuery != null && booleanQuery.length() > 0) {
				// limit the result set to the set yielded by the boolean query
				potentialDocumentIds = this.processBooleanQuery(booleanQuery);
				if (query.equals("")) {
					// no keywords left => return the result of the boolean query
					if (potentialDocumentIds != null) {
						if (potentialDocumentIds.size() <= topK) {
							return potentialDocumentIds;
						} else {
							for (int i = 0; i < topK; i++) {
								result.add(potentialDocumentIds.get(i));
							}
							return result;
						}
					}
					return result;
				}
			} // else: no limitiation
			
			if (prf == 0) {
				// no pseudo relevance feedback
				
				// get the IDs of the topK most relevant documents
				result = this.processInnerBM25Query(terms, topK, potentialDocumentIds);
			} else {
				// pseudo relevance feedback
				
				// get the IDs of the prf most relevant documents
				ArrayList<Long> ids = this.processInnerBM25Query(terms, prf, potentialDocumentIds);
				
				// get the snippets
				ArrayList<String> snippets = this.createQueryAnswerForDocuments(ids, query);
				
				// use the snippets to expand the query
				terms = this.expandQueryTerms(terms, snippets);
				
				// reevaluate the expanded query and get the topK most relevant documents
				result = this.processInnerBM25Query(terms, topK, potentialDocumentIds);
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
			snippet = snippet.replaceAll("...", "");	// added during the snippet creation, no significance
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
	 * {@link #processKeywordQuery(String, int, int)}.
	 * @param potentialDocumentIds list of documents which may be used during
	 *   the scoring
	 */
	private ArrayList<Long> processInnerBM25Query(List<String> terms, int topK, List<Long> potentialDocumentIds) {
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
			// add (term, termList) to the map
			Index.TermList termList = this.indexHandler.readListForTerm(term, false);
			if (termList != null) {
				termListMap.put(term, termList);
			} else {
				// if the loading is aborted (too many entries), add an empty list
				termListMap.put(term, new Index.TermList());
			}
		}

		/*
		 * Compute variable n (number of documents containing a term) per 
		 * term by parsing the occurrences. Use this opportunity to compute
		 * variable qf (frequency of term in the query) per term. Also get the
		 * set of all documents containing any query term.
		 */
		Map<String, Integer> termDocumentCountMap = new HashMap<String, Integer>();
		Map<String, Integer> termQueryFrequency = new HashMap<String, Integer>();
		Set<Long> documentIds = new TreeSet<Long>();	// no repetitions
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
		// if there are any, add the potential document IDs to the set
		if (potentialDocumentIds != null) {
			documentIds.addAll(potentialDocumentIds);
		}

		// get N (the total number of documents)
		final int N = this.indexHandler.totalNumberOfDocuments();

		// get the set of query terms (without duplicates)
		Set<String> uniqueTerms = new HashSet<String>();
		uniqueTerms.addAll(terms);

		// Rank each document which contains at least one query term
		Map<Double, Long> scoreDocumentMap = new TreeMap<Double, Long>();	// ordered by score
		boolean scoreListFull = false;
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
				score -= 1e-10 * Math.random();	// slightly decrease the score (this is sloppy, but works)
			}

			// store the score - only keep the topK best scores
			if (scoreListFull) {
				// get the lowest score (there must be at least 1 score already)
				double lowestScore = (Double) scoreDocumentMap.keySet().toArray()[0];
				// if the new score is greater, drop the lowest
				scoreDocumentMap.remove(lowestScore);
				// add the new score (TreeMap sorts automatically)
				scoreDocumentMap.put(score, documentId);
			} else {
				// keep adding documents until topK is reached
				scoreDocumentMap.put(score, documentId);
				if (scoreDocumentMap.keySet().size() >= topK) {
					scoreListFull = true;
				}
			}
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
	
	/**
	 * Compute the normalized distributed cumulative gain using the gold ranking
	 * and the actual ranking, up to a given rank.
	 * @param goldRanking the gold ranking
	 * @param ranking the ranking (snippets may be used here, see 
	 *   {@link #getTitleFromQueryAnswer(String)})
	 * @param at the desired rank
	 * @return the NDCG
	 */
	@Override
	Double computeNdcg(ArrayList<String> goldRanking, ArrayList<String> ranking, int at) {
		double dcg = 0.0;
		double idcg = 0.0;
		int rank = 1;

		Iterator<String> iter = ranking.iterator();
		while (rank <= at) {
			if (rank == 1) {
				idcg += 1 + Math.floor(10 * Math.pow(0.5, 0.1 * rank));
			} else {
				idcg += 1 + Math.floor(10 * Math.pow(0.5, 0.1 * rank)) / Math.log(rank);
			}
			if (iter.hasNext()) {
				String title = this.getTitleFromQueryAnswer(iter.next());
				int origRank = goldRanking.indexOf(title) + 1;
				if (origRank < 1) {
					rank++;
					continue;
				} else if (rank == 1) {
					dcg += 1 + Math.floor(10 * Math.pow(0.5, 0.1 * origRank));
					rank++;
					continue;
				} else {
					dcg += 1 + Math.floor(10 * Math.pow(0.5, 0.1 *origRank)) / Math.log(rank);
				}
			}
			rank++;
		}
		return dcg / idcg;
	}
	
}
