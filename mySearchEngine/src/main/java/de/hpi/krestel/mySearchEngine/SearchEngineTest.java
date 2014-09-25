package de.hpi.krestel.mySearchEngine;

// Execute with 2GB RAM (java -Xmx2g).

public class SearchEngineTest {

	static String[] queries = {
		"LINKTO goldenehimbeere",
		"LINKTO Journal of Inorganic and Nuclear Chemistry",
		"Actinium Uranisotop",
		"China OR Smithee",
		"China AND Smithee",
		"China BUT NOT Smithee",
		"Ang Lee AND Puppenhaus",
		"\"Alan Smithee\" AND Schreibweise",
		"Patr*",
		"Alan AND Smith* OR \"namensgebende Element\" BUT NOT Harlekin",
		"",
	};
	
	/* final test:

	static String[] queries = {
		"LINKTO goldenehimbeere",
		"\"ein trauriges Arschloch\"", 
		"Toskana AND Wein", 
		"sülz* AND staatlich", 
		"öffentlicher nahverkehr stadtpiraten", 
		"schnitzel AND kaffe BUT NOT schwein*", 
		"Dr. No", 
		"ICE BUT NOT T", 
		"Bierzelt Oktoberfest", 
		"Los Angeles sport", 
		"08/15"
	}; 

	 */
	
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
