package de.hpi.krestel.mySearchEngine;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.xml.bind.DatatypeConverter;

/**
 * An Index holds a map which maps terms to TermLists (which hold the
 * occurrences of that term in documents).
 * Index uses {@link TreeMap}, whose keys are ordered, to provide the
 * ordering of terms.
 * This is a utility class. It does not check for null values.
 */
class Index {
	
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
	 * Alternatively, TermList can be used to store lists of links to a
	 * particular page. In that case, the "term" must be the processed
	 * document title (it has to be ensured that it will not interfere with
	 * actual terms) of the document, and the position lists will only be
	 * used to store the document IDs (i.e., no position or a dummy position
	 * will be stored for each document which links to the document).
	 */
	public static class TermList {
		
		/**
		 * The map of occurrences for this list's term.
		 * We use Collection instead of List because we want to use
		 * TreeSets, which do not implement the interface List. Iterate
		 * over the elements via {@link Collection#iterator()}.
		 */
		private Map<Long, Collection<Integer>> occurrences;
		private static final byte[] semi = ";".getBytes();
		private static final byte[] colon = ":".getBytes();
		private static final byte[] comma = ",".getBytes();
		private static final byte[] dot = ".".getBytes();
		
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
		public static Index.TermList createFromIndexString(String string) {
			Index.TermList list = new TermList();
			
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
			Collection<Integer> positions = this.createCollectionForDocument(documentId);
			
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
		private Collection<Integer> createCollectionForDocument(Long documentId) {
			Collection<Integer> result = this.occurrences.get(documentId);
			if (result == null) {
				result = new TreeSet<Integer>();
				this.occurrences.put(documentId, result);
			}
			return result;
		}
		
		/**
		 * Provide a String representation for this TermList which is
		 * suited for human-readable output. For indexing, use
		 * 
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
		 * @throws IOException 
		 */
		public void toIndexString(BufferedOutputStream bo, String term, boolean isIndexing) throws IOException {
			boolean isFirstOccurence = true;
			boolean isFirstPosition = true;
			for (Long documentId : this.occurrences.keySet()) {	// uses iterator
				if (isFirstOccurence && isIndexing) {
					// encode term as base64 to avoid .,-: etc...
					bo.write(DatatypeConverter.printBase64Binary(term.getBytes()).getBytes());
					bo.write(Index.TermList.colon);
				}
				if (!isFirstOccurence) bo.write(Index.TermList.semi);
				bo.write(documentId.toString().getBytes());
				bo.write(Index.TermList.colon);
				Collection<Integer> positions = this.occurrences.get(documentId);
				if (positions != null) {
					isFirstPosition = true;
					for (Integer position : positions) {	// uses iterator
						if (!isFirstPosition) bo.write(Index.TermList.comma);
						bo.write(position.toString().getBytes());
						isFirstPosition = false;
					}
				}
				isFirstOccurence = false;
			}
			bo.write(Index.TermList.dot);
			if (isIndexing) bo.write("\n".getBytes());
			bo.flush();
		}
		
		public Map<Long, Collection<Integer>> getOccurrences() {
			return this.occurrences;
		}
		
	}
	
	private Map<String, Index.TermList> termLists;
	
	/**
	 * Create a new Index.
	 * Initialize the map.
	 */
	public Index() {
		this.termLists = new TreeMap<String, Index.TermList>();
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
		Index.TermList list = this.createListForTerm(term);
		// delegate the rest to the TermList
		list.addOccurrence(documentId, position);
	}
	
	/**
	 * Creates a new TermList for the given term and adds it to the map,
	 * using the term as key.
	 * If the term is already present in the map, nothing will be changed.
	 * @param term the term to create a list for
	 */
	private Index.TermList createListForTerm(String term) {
		Index.TermList list = this.termLists.get(term);
		if (list == null) {
			list = new TermList();
			this.termLists.put(term, list);
		}
		return list;
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
			Index.TermList list = this.termLists.get(term);
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
	public Map<String, Index.TermList> getTermLists() {
		return this.termLists;
	}
	
}