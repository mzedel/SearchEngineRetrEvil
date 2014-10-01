package de.hpi.krestel.mySearchEngine;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.SocketTimeoutException;
import java.net.UnknownServiceException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class SearchEngine {

	String baseDirectory = "/Users/tim/Documents/Studium/Master/SoSe 2014/Information Retrieval/Engine/wikipedia/";
	String wikiDirectory;
	String directory;
	String logFile;

	public SearchEngine() {
		// Directory to store index and result logs
		this.directory = this.baseDirectory +this.getClass().getSimpleName().toString();
		new File(this.directory).mkdirs();
		this.logFile = this.directory +"/" +System.currentTimeMillis() +".log";
		// Directory to store wikipedia results
		this.wikiDirectory = this.baseDirectory +"wikiQueryResults/";
		new File(this.wikiDirectory).mkdirs();
	}

	void indexWrapper() {
		long start = System.currentTimeMillis();
		if (!loadIndex(this.directory)) {
			index(this.directory);
			loadIndex(this.directory);
		}
		long time = System.currentTimeMillis() - start;
		log("Index Time: " +time +"ms");
	}


	void searchWrapper(String query, int topK, int prf) {
		long start = System.currentTimeMillis();
		ArrayList<String> ranking = search(query, topK, prf);
		long time = System.currentTimeMillis() - start;
		ArrayList<String> goldRanking = getGoldRanking(query);
		Double ndcg = computeNdcg(goldRanking, ranking, topK);
		String output = "\nQuery: " + query +"\t Query Time: " + time +"ms\nRanking: ";
		System.out.println("query: " + query);
		if (ranking!=null) {
			Iterator<String> iter = ranking.iterator();
			while(iter.hasNext()){
				String item = iter.next();
				output += item +"\n";
			}
		}
		output += "\nnDCG@" +topK +": " +ndcg;
		log(output);
	}

	@SuppressWarnings("unchecked")
	ArrayList<String> getGoldRanking(String query) {
		ArrayList<String> gold;
		String queryTerms = query.replaceAll(" ", "+");
		try {
			FileInputStream streamIn = new FileInputStream(this.wikiDirectory + queryTerms.replaceAll("/","-") + ".ser");
			ObjectInputStream objectinputstream = new ObjectInputStream(streamIn);
			gold = (ArrayList<String>) objectinputstream.readObject();
			objectinputstream.close();
			return gold;
		} catch (Exception ex) {}

		gold = new ArrayList<String>();
		String url = "http://de.wikipedia.org/w/index.php?title=Spezial%3ASuche&search=" + queryTerms + "&fulltext=Search&profile=default";
		String wikipage = "";	
		try {
			wikipage = (String) new WebFile(url).getContent();
		} catch (SocketTimeoutException e) {
			e.printStackTrace();
		} catch (UnknownServiceException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String[] lines = wikipage.split("\n");
		for (int i=0;i<lines.length;i++) {
			if (lines[i].startsWith("<li>")) {
				/*
				 * The original pattern 
				 * 		title=\"(.*)\"
				 * broke for redirections (which included two occurrences of 
				 * 		title="..."
				 * so that the match included everything between the first '"' of
				 * the first occurrence and the second '"' of the second occurrence.
				 * Therefore, the pattern
				 * 		title=\"([^\"]*)\"
				 * is used to exclude '"'.
				 */
				Pattern p = Pattern.compile("title=\"([^\"]*)\"");
				Matcher m = p.matcher(lines[i]);
				m.find();
				gold.add(m.group(1));
			}
		}		
		try {
			FileOutputStream fout = new FileOutputStream(this.wikiDirectory + queryTerms.replaceAll("/","-") + ".ser");
			ObjectOutputStream oos = new ObjectOutputStream(fout);
			oos.writeObject(gold);
			oos.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return gold;
	}

	synchronized void log(String line) {
		System.out.println(">>> " + line);
		try {
			FileWriter fw = new FileWriter(this.logFile,true);
			BufferedWriter out = new BufferedWriter(fw);
			out.write(line +"\n");
			out.close();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}	

	abstract boolean loadIndex(String directory);

	abstract void index(String directory);

	abstract ArrayList<String> search(String query, int topK, int prf);

	abstract Double computeNdcg(ArrayList<String> goldRanking, ArrayList<String> myRanking, int at);
	
}
