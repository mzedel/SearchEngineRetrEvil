package de.hpi.krestel.mySearchEngine;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

// Don't change this file!
public abstract class SearchEngine {

	String directory;
	String logFile;

	public SearchEngine() {

		// Directory to store index and result logs 
		// TODO: change back to "/home/krestel/data/wikipedia-de/"
		this.directory = "/Users/tim/Documents/Studium/Master/SoSe 2014/Information Retrieval/Aufgaben/wikipedia/" + this.getClass().getSimpleName().toString();
		new File(directory).mkdirs();
		this.logFile = this.directory +"/" +System.currentTimeMillis() +".log";

	}

	void indexWrapper(){

		long start = System.currentTimeMillis();
		if(!loadIndex(this.directory)){
			index(this.directory);
			loadIndex(this.directory);
		}
		long time = System.currentTimeMillis() - start;
		log("Index Time: " +time +"ms");
	}

	
	void searchWrapper(String query, int topK, int prf){

		long start = System.currentTimeMillis();
		ArrayList<String> ranking = search(query, topK, prf);
		long time = System.currentTimeMillis() - start;
		Double ndcg = computeNdcg(query, ranking, topK);
		String output = "\nQuery: " +query +"\t Query Time: " +time +"ms\nRanking: ";
		System.out.println("query: " +query);
		if(ranking!=null){
			Iterator<String> iter = ranking.iterator();
			while(iter.hasNext()){
				String item = iter.next();
				output += item +"\n";
				System.out.println(item);
			}
		}
		output += "\nnDCG@" +topK +": " +ndcg;
		log(output);
	}

	synchronized void log(String line) {

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

	abstract Double computeNdcg(String query,  ArrayList<String> ranking, int at);
}
