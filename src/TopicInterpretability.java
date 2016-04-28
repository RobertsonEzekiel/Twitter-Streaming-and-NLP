/*
 * TopicInterpretability
 * 11/4/2016
 * Ezekiel Robertson
 * 
 * Calculates the interpretability of a topic based on 'Machine Reading Tea
 * Leaves: Automatically Evaluating Topic Coherence and Topic Model Quality'
 * (Jey Han Lau, David Newman, Timothy Baldwin). We will be using the Observed
 * Coherence Automatic Normalized Pointless Mutual Information method 
 * (OC-Auto-NPMI).
 */

import java.util.ArrayList;
import java.util.HashMap;
//import java.util.Map;

public class TopicInterpretability {
	String collcSep = "_";
	int windowTotal = 0;
	HashMap<String, Integer> wordCount = new HashMap<String, Integer>();
	
	
	public double calcAssoc(String word1, String word2) {
		double result;
		String combined1 = new String(word1 + "|" + word2);
		String combined2 = new String(word2 + "|" + word1);
		int combinedCount = 0, w1Count = 0, w2Count = 0;
		
		if (wordCount.containsKey(combined1)) {
			combinedCount = wordCount.get(combined1);
		} else if (wordCount.containsKey(combined2)) {
			combinedCount = wordCount.get(combined2);
		}
		
		if (wordCount.containsKey(word1)) {
			w1Count = wordCount.get(word1);
		}
		if (wordCount.containsKey(word2)) {
			w2Count = wordCount.get(word2);
		}
		
		if (w1Count == 0 || w2Count == 0 || combinedCount == 0) {
			result = 0.0;
		} else {
			result = Math.log10((combinedCount * windowTotal) / (w1Count * w2Count));
			result = result / (-1.0 * Math.log10(combinedCount / windowTotal));
		}
		
		return result;
	}
	
	public double calcTopicCoherence (String[] topicWords) {
		ArrayList<Double> topicAssoc = new ArrayList<Double>();
		double topicAssocSum = 0.0;
		
		for (int w1id = 0; w1id < topicWords.length - 1; w1id++) {
			String targetWord = topicWords[w1id];
			String w1 = " " + targetWord.split(collcSep);
			
			for (int w2id = w1id + 1; w2id < topicWords.length; w2id++) {
				String topicWord = topicWords[w2id];
				String w2 = " " + topicWord.split(collcSep);
				
				if (targetWord != topicWord) {
					topicAssoc.add(calcAssoc(w1, w2));
				}
			}
		}
		
		return topicAssocSum / topicAssoc.size();
	}
}
