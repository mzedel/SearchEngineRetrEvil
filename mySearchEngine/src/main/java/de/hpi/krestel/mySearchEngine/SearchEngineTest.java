package de.hpi.krestel.mySearchEngine;

// This file will be used to evaluate your search engine!
// You can use/change this file for development. But
// any changes you make here will be ignored for the final test!

// You can use and are encouraged to use multi-threading, map-reduce, etc for
// indexing and/or searching
// The final evaluation will be done with 2GB RAM (java -Xmx2g) !!!!!!!!!!

public class SearchEngineTest {

	// Some test queries for development. The real test queries will be more difficult ;) 
	static String[] queries = {"artikel", "deutsch"};
	
	// some variables (will be explained when needed, ignore for now!)
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
	
	/*
	 * Stuff from earlier weeks (not relevant anymore, just for reference)
	 */
	
	/*
	 * Count the number of pages in the actual Wikipedia dump.
	 * Not requested, just to test streaming.
	 * Result: 3317876 - streaming seems to work.
	 */
//	private static void week1SequentialTest() throws Exception {
//		SAXParser saxParser = SAXParserFactory.newInstance().newSAXParser();
//		DefaultHandler saxHandler = new DefaultHandler() {
//
//			long pageCount = 0;
//
//			@Override
//			public void endElement(String uri, String localName, String qName)
//					throws SAXException {
//				if (qName.equalsIgnoreCase("page")) {
//					pageCount++;
//				}
//			}
//			
//			@Override
//			public void endDocument() throws SAXException {
//				System.out.println("Wikipedia dump: number of pages: " + pageCount);
//			}
//			
//		};
//		
//		String filePath = getDumpPath("deWikipediaDump.xml");
//		
//		saxParser.parse(filePath, saxHandler);
//	}
	
//	private static String getDumpPath(String dumpFileName) {
//		return (new java.io.File(SearchEngineTest.class
//			.getResource("SearchEngineTest.class").getPath())
//			.getParentFile()
//			.getParentFile()
//			.getParentFile()
//			.getParentFile()
//			.getParentFile()
//			.getParentFile()					// mySearchEngine/target
//			.getParentFile()					// mySearchEngine
//			.getParentFile().getPath()			// mySearchEngine/..
//			+ "/wikipedia/" + dumpFileName);
//	}
	
	/*
	 * Week 1: print titles and page IDs of the pages and the development dump.
	 */
//	private static void week1SAX() throws SAXException, 
//			ParserConfigurationException, IOException {
//		// create SAX parser using default parser factory
//		SAXParserFactory factory = SAXParserFactory.newInstance();
//		SAXParser saxParser = factory.newSAXParser();
//
//		// create SAX handler by subclassing DefaultHandler
//		DefaultHandler saxHandler = new DefaultHandler() {
//			
//			// event handlers
//
//			/*
//			 * state regarding current page
//			 * 
//			 * per page, output 'page - title: "..." - id: "..."'; make sure
//			 * that only IDs of pages (not of revisions) are printed
//			 */
//			boolean inPage, inTitle, inId, firstId = false;
//			String result = "";
//
//			@Override
//			public void startElement(String uri, String localName,
//					String qName, Attributes attributes) throws SAXException {
//				if (qName.equalsIgnoreCase("page")) {
//					inPage = true;
//					firstId = true;
//					result = "page";
//				} else if (inPage && qName.equalsIgnoreCase("title")) {
//					inTitle = true;
//					result += " - title: \"";
//				} else if (inPage && firstId && qName.equalsIgnoreCase("id")) {
//					inId = true;
//					result += " - id: \"";
//				}
//			}
//			
//			@Override
//			public void endElement(String uri, String localName, String qName)
//					throws SAXException {
//				if (qName.equalsIgnoreCase("page")) {
//					inPage = false;
//					System.out.println(result);
//				} else if (inPage && qName.equalsIgnoreCase("title")) {
//					inTitle = false;
//					result += "\"";
//				} else if (inPage && firstId && qName.equalsIgnoreCase("id")) {
//					inId = false;
//					firstId = false;
//					result += "\"";
//				}
//			}
//			
//			@Override
//			public void characters(char[] ch, int start, int length)
//					throws SAXException {
//				if (inTitle || inId) {
//					result += new String(ch, start, length);
//				}
//			}
//			
//		};
//		
//		// get XML file path
//		String filePath = getDumpPath("testDump.xml");
//		
//		// parse file
//		saxParser.parse(filePath, saxHandler);
//	}

}
