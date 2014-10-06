package de.hpi.krestel.mySearchEngine;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

class LinkIndex {

	public static class TitleList {

		public static final byte[] semi = ";".getBytes();
		public static final byte[] colon = ":".getBytes();
		public static final byte[] comma = ",".getBytes();
		public static final byte[] dot = ".".getBytes();
		
		String title;
		private Collection<String> titles;
		
		public TitleList(String title) {
			this.title = title;
			this.titles = new TreeSet<String>();
		} 
		
		public static LinkIndex.TitleList createFromIndexString(String string) {
			LinkIndex.TitleList list = new TitleList(null);
			
			// format: title:othertitle,othertitle,othertitle[.]
			StringTokenizer tok = new StringTokenizer(string, ":,.");
			boolean firstToken = true;
			while (tok.hasMoreTokens()) {
				String token = tok.nextToken();
				if (firstToken) {
					list.title = token;
					firstToken = false;
				} else {
					list.addTitle(token);
				}
			}
			return list;
		}
		
		public void addTitle(String linkingTitle) {
			if (!titles.contains(linkingTitle)) {
				titles.add(linkingTitle);
			}
		}
		
		public String toString() {
			StringBuilder result = new StringBuilder();
			
			result.append(this.title + ": ");
			result.append("( ");
			for (String title : this.titles) {
				result.append("\"" + title + "\" ");
			}
			result.append(")");
			
			return result.toString();
		}

		// format: title:othertitle,othertitle,othertitle.\n
		public void toIndexString(BufferedOutputStream bo) throws IOException {
			if (title == null || title.isEmpty() || titles.isEmpty()) return;
			// write title
			bo.write(title.getBytes());
			bo.write(colon);
			// write other titles
			boolean isFirstTitle = true;
			for (String titre : titles) {
				if (!isFirstTitle) {
					bo.write(comma);
				} else {
					isFirstTitle = false;
				}
				bo.write(titre.getBytes());
			}
			
			// finish
			bo.write(dot);
			bo.write("\n".getBytes());
			bo.flush();
		}
		
		public Collection<String> getTitles() {
			return this.titles;
		}
		
	}
	
	private Map<String, LinkIndex.TitleList> titleLists;
	

	public LinkIndex() {
		this.titleLists = new TreeMap<String, LinkIndex.TitleList>();
	}
	
	public void addLinkingTitle(String title, String linkingTitle) {
		LinkIndex.TitleList list = this.getListForTitle(processTitle(title));
		list.addTitle(processTitle(linkingTitle));
	}
	
	public static String processTitle(String title) {
		return title.trim().toLowerCase().replaceAll("[ .:,;-]", "");
	}
	
	private LinkIndex.TitleList getListForTitle(String processedTitle) {
		LinkIndex.TitleList list = this.titleLists.get(processedTitle);
		if (list == null) {
			list = new TitleList(processedTitle);
			this.titleLists.put(processedTitle, list);
		}
		return list;
	}
	
	public String toString() {
		StringBuilder result = new StringBuilder();
		
		result.append("{\n");
		for (String term : this.titleLists.keySet()) {
			result.append(term + ": ");
			LinkIndex.TitleList list = this.titleLists.get(term);
			if (list != null) {
				result.append(list + "\n");
			}
		}
		result.append("}");
		
		return result.toString();
	}
	
	public Map<String, LinkIndex.TitleList> getTitleLists() {
		return this.titleLists;
	}
	
}