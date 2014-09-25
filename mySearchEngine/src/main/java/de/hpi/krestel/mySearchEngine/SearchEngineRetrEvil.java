package de.hpi.krestel.mySearchEngine;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
 * Describe your search engine briefly:
 *  - multi-threaded?
 *  - stemming?
 *  - stopword removal?
 *  - index algorithm?
 *  - etc.
 *  
 *  - We use Lucene for pre-processing documents and queries (lower case,
 *    tokenizing, stemming, stopword removal using a newer german list)
 *  - Redirection pages are not indexed to begin with. Therefore, they are not
 *    considered in the ranking (of other documents) either.
 */
public class SearchEngineRetrEvil extends SearchEngine {
	
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
	 * Whether snippets shall be included in the result set.
	 * If set to <tt>true</tt>, only titles will be returned.
	 * If set to <tt>false</tt>, for each document, the title followed by the
	 * snippet will be returned.
	 */
	private static final boolean PRINT_SNIPPETS = true;
	
	/**
	 * Index handler for queries etc.
	 */
	private IndexHandler indexHandler;
	
	public SearchEngineRetrEvil() {
		// This should stay as is! Don't add anything here!
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
			InputSource is = new InputSource(new FileInputStream(dumpFile));
//			is.setEncoding("ISO-8859-15");
			// parse the dump
			saxParser.parse(is, saxHandler);
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

	/*
	 * TODO: combine prefix queries, phrase queries, keyword queries
	 */
	@Override
	ArrayList<String> search(String query, int topK, int prf) {
		// invalid arguments: return an empty result set
		if (query == null || topK <= 0) {
			return new ArrayList<String>();
		}
		
		// answer query depending on its type
		/*if (isBooleanQuery(query)) {
			return processBooleanQuery(query);			// boolean query
		} else*/ if (query.contains("LINKTO ")) {
			return processLinkQuery(query);				// link query
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
			String title = this.indexHandler.getIdsToTitles().get(documentId);
			
			if (PRINT_SNIPPETS) {
				// get a snippet of the document
				String snippet = this.indexHandler.getSnippetForDocumentId(documentId, queryTerms);
				
				// store: title + newline (unless snippet is null) + snippet
				result.add((title != null ? title : "") 
						+ (snippet != null ? ("\n" + snippet) : ""));
			} else {
				// store the title only
				result.add(title != null ? title : "");
			}
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
		for(Entry<String, Long> entry : this.indexHandler.getSeeklist().entrySet()) {
			if(entry.getKey().startsWith(prefix))
				results.add(this.indexHandler.getIdsToTitles().get(entry.getValue()));
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
					results.add(this.indexHandler.getIdsToTitles().get(document));
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
				results.add(this.indexHandler.getIdsToTitles().get(document));
			}
		} else if (query.contains(SearchEngineRetrEvil.BUT_NOT)) {
			int index = Arrays.asList(terms).indexOf(SearchEngineRetrEvil.BUT_NOT);
			String leftProcessedTerm = processBooleanQuery(Arrays.asList(terms).get(index - 1)).get(0);
			String rightProcessedTerm = processBooleanQuery(Arrays.asList(terms).get(index + 1)).get(0);
			
			Index.TermList leftTermList = this.indexHandler.readListForTerm(leftProcessedTerm);
			Index.TermList rightTermList = this.indexHandler.readListForTerm(rightProcessedTerm);
			for (Long document : leftTermList.getOccurrences().keySet()) {
				if (!rightTermList.getOccurrences().containsKey(document)) {
					results.add(this.indexHandler.getIdsToTitles().get(document));
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
	
	private ArrayList<String> processLinkQuery(String query) {
		// pre-process the title
		String processedTitle = LinkIndex.processTitle(query.replace("LINKTO ", "").trim());
		// try to read the TitleList (may be null)
		LinkIndex.TitleList titleList = this.indexHandler
				.readListForTitle(processedTitle);
		
		// get the IDs of documents linking to the title
		List<Long> documentIds = new ArrayList<Long>();
		if (titleList != null) {
			for (String listedTitle : titleList.getTitles()) {
				// listedTitle is the pre-processed title, need the document ID
				Long documentId = this.indexHandler.getTitlesToIds().get(listedTitle);
				if (documentId != null && !documentIds.contains(documentId)) {
					documentIds.add(documentId);
				}
			}
		}
		
		// get the snippets of the documents linking to the title
		return this.createQueryAnswerForDocuments(documentIds, new ArrayList<String>());
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
				String title = this.indexHandler.getIdsToTitles().get(documentId);
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

	/**
	 * Compute the Normalized Discounted Cumulative Gain for the given ranking
	 * and ideal ranking at the given position.<br>
	 * 
	 * Relevance: assume binary relevance, i.e., all documents in the goldRanking
	 * are relevant and documents in the actual ranking are relevant if and only
	 * if they are also listed in the goldRanking.
	 * @param goldRanking the ideal ranking of document titles
	 * @param myRanking the actual ranking of document titles
	 * @param at the rank for which the value is to be computed, counting
	 *   from 1 (highest ranking document), or <tt>null</tt> if an argument is
	 *   invalid (i.e., at < 1 or goldRanking or myRanking are <tt>null</tt> or 
	 *   empty); the actual rank which is used is min{at, goldRanking.size, myRanking.size}
	 */
	@Override
	Double computeNdcg(ArrayList<String> goldRanking, ArrayList<String> myRanking, int at) {
		// catch invalid arguments
		if (goldRanking == null || goldRanking.size() == 0 
				|| myRanking == null || myRanking.size() == 0
				|| at < 1) {
			return null;
		}
		at = Math.min(Math.min(goldRanking.size(), myRanking.size()), at);
		
		// create list of relevance values for the actual ranking
		double[] actualRelevance = new double[at];	// up to the desired rank
		for (int i = 0; i < at; i++) {
			/* 
			 * assume binary relevance where a document is relevant if and only
			 * if it is in the ideal ranking, regardless of position
			 */
			actualRelevance[i] = goldRanking.contains(myRanking.get(i)) ? 1.0 : 0.0;
		}
		
		// create list of relevance values for the ideal ranking
		double[] idealRelevance = new double[at];	// up to the desired rank
		for (int i = 0; i < at; i++) {
			/*
			 * assume binary relevance where every document within the ideal
			 * ranking is relevant
			 */
			idealRelevance[i] = 1;
		}
		return ndcg(actualRelevance, idealRelevance);
	}
	
	/**
	 * Helper method which computes the Normalized Discounted Cumulative Gain
	 * for arrays of relevance values from the actual and an ideal ranking at
	 * the last position.
	 * @param actualRelevance relevance values for the actual ranking
	 * @param idealRelevance relevance values for the ideal ranking
	 * @return the NDCG at the last position
	 */
	private static double ndcg(double[] actualRelevance, double[] idealRelevance) {
		// initialize DCGs
		double actualDcg = 0.0;
		double idealDcg = 0.0;
		
		// compute the DCGs
		for (int i = 0; i < actualRelevance.length; i++) {
			actualDcg += (actualRelevance[i] * gain(i + 1));
			idealDcg += (idealRelevance[i] * gain(i + 1));
		}
		
		// return the NDCG
		return actualDcg / idealDcg;
	}
	
	/**
	 * Helper method which computes the exponentially decaying gain value of
	 * a document at the given rank.
	 * @param rank the rank of the document, starting with 1
	 * @return the gain value within [1, 10]
	 */
	private static int gain(int rank) {
		return 1 + ((int) Math.floor(10.0 * Math.pow(0.5, 0.1 * rank)));
	}
	
}
