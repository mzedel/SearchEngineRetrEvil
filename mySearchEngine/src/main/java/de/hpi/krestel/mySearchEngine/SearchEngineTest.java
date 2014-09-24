package de.hpi.krestel.mySearchEngine;

// Execute with 2GB RAM (java -Xmx2g).

public class SearchEngineTest {

	static String[] queries = {"LINKTO goldenehimbeere"};
	
	static int topK = 10;
	static int prf = 5;
	
	public static void main(String[] args) {
		// Evaluate SearchEngineRetrEvil
		SearchEngine se = new SearchEngineRetrEvil();
		evaluate(se);
	}

	private static void evaluate(SearchEngine se) {
		// Load or generate the index
		se.indexWrapper();

		for (int i=0; i<SearchEngineTest.queries.length; i++) {
			// Search and store results
			se.searchWrapper(queries[i], topK, prf);
		}		
	}

}
