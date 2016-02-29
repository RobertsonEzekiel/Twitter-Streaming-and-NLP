/*
 * WordFrequencyCounter.java
 * 17/2/2016
 * Ezekiel Robertson
 * 
 * A twitter stream that cleans, tokenizes and counts each word as it comes in.
 * Outputs a list of words sorted in descending order by count. This way I can
 * assemble my own list of stopwords for use in the main topic modeling program.
 */

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Map.*;
import java.io.*;
import java.util.HashMap;

public class WordFrequencyCounter {
	// We only want so many tweets to be read.
	static int countTweets = 0;
	static int MAXTWEETS = 50000;
	// Set all tokens to lower case to reduce vocabulary size. 
	static boolean lowercase = true;
	// Full file path to the model used by the POS tagger.
	static String MODELNAME = "C:\\Users\\User\\Documents\\Zeke\\model.20120919.txt";
	// Tags for POS to be removed. See arc-tweet-nlp-0.3.2\docs\annot_guidelines.md
	// for the meanings of each tag.
	static List<String> REMOVE = Arrays.asList(",","@","U","E","G","~");
	// Index of numbers corresponding to each word. Used for the mapping. 
	static int index = 0;
	
	public static void main(String[] args) throws IOException{
		// Configure OAuth keys/tokens for "Zeke's Test Stream" application. If
		// the application is changed in any way on apps.twitter.com, these
		// OAuth keys will also change, and the strings will need to be edited.
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true);
		cb.setOAuthConsumerKey("8Nka5YKCKKT2mq53eZV0sN9IW");
		cb.setOAuthConsumerSecret("IgziRUuminrSOdOmwDfqVJ0fhNqeJW2lqaHIU0lNMEtFIx4RGS");
		cb.setOAuthAccessToken("4805189297-SmHEh2MaoKsElGOgEnjKJr7IpV6cPRBZHDEHBEp");
		cb.setOAuthAccessTokenSecret("gZXMZ6DG2vMwOQeJSWXJvbJkcWmruxWbDoJ9Cutnd3qx2");
		cb.setPrettyDebugEnabled(true);
		cb.setHttpRetryCount(5);
		
		// Open a file to store the word counts.
		File wordFile = new File("C:\\Users\\User\\test_olda\\WordCount.txt");
		
		if (!wordFile.exists()) {
			wordFile.createNewFile();
		}
		FileOutputStream wordFOS = new FileOutputStream(wordFile);
		OutputStreamWriter wordOSW = new OutputStreamWriter(wordFOS);
		
		//Load and initialize the POS tagger. Part of Tagger.java.
		Tagger tagger = new Tagger();
		try {
		tagger.loadModel(MODELNAME);
		}
		catch (IOException mod) {
			System.err.println("Could not load model at " + MODELNAME);
		}
		
		// Mapping from each word to an integer, for ease of sorting/counting.
		HashMap<Integer, String> vocab = new HashMap<Integer, String>();
		ArrayList<Integer> counts = new ArrayList<Integer>();
		
		// Listen to the stream, keeping track of each word as it comes in,
		// incrementing its count by 1.
		StatusListener listener = new StatusListener() {
			// Someone sent a tweet we can look at. Yay!
			@Override
			public void onStatus(Status status) {
				// The location filter does not do all of the work we need.
				// Make sure we only use geotagged tweets written in English
				//if ((status.getGeoLocation() != null) && (status.getLang().equals("en"))) {
					// Store the text part of the tweet.
					String text = status.getText();
					// Set everything to lowercase to lessen the number of
					// unique character stings.
					if (lowercase) {
						text = text.toLowerCase();
					}
					// Tokenize and POS tag it for cleaning up
					List<Tagger.TaggedToken> taggedTokens = tagger.tokenizeAndTag(text);
					
					int temp;
					// Build the vocabulary map and word counts.
					for (Tagger.TaggedToken token : taggedTokens) {
						// Remove any emoticons, punctuation, @mentions, and
						// other clutter, and check if the word is already in
						// our vocabulary.
						if (!REMOVE.contains(token.tag)) {
							if (!vocab.containsValue(token.token)) {
								vocab.put(index, token.token);
								counts.add(index, 1);
								index++;
							}
							else {
								int key = getKeysByValue(vocab, token.token);
								temp = counts.get(key);
								temp++;
								counts.set(key, temp);
							}
						}
					}
					countTweets++;
					Integer[] countArray = new Integer[counts.size()];
					countArray = counts.toArray(countArray);
					String tempString;
					int tempInt;
					if (countTweets >= MAXTWEETS) {
						// Once enough tweets have been gathered, sort and print the
						// list. Uses a bubble algorithm to sort the map based
						// on how the count array gets sorted.
						for (int ii = 0; ii < countArray.length - 1; ii++) {
							for (int jj = 1; jj < countArray.length - ii; jj++) {
								if (countArray[jj - 1] < countArray[jj]) {
									tempInt = countArray[jj - 1];
									countArray[jj - 1 ] = countArray[jj];
									countArray[jj] = tempInt;
									tempString = vocab.get(jj - 1);
									vocab.replace(jj - 1, vocab.get(jj));
									vocab.replace(jj, tempString);
								}
							}
						}
						// Print to file
						try {
							for (int kk = 0; kk < countArray.length; kk++) {
								wordOSW.write(vocab.get(kk) + "\t" + 
										countArray[kk] + "\n");
							}
						wordOSW.close();
						} catch(IOException ioe) {
							ioe.printStackTrace();
						}
						
						System.exit(0);
					}
				//}
			}

			// Someone has deleted one of their previous messages.
			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				System.out.println("Got a status deletion notice id:"
						+ statusDeletionNotice.getStatusId());
			}
			// Warning of limitations.
			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got track limitation notice:"
						+ numberOfLimitedStatuses);
			}
			// Someone has deleted their location from a geotagged tweet.
			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("Got scrub_geo event userId:" + userId
						+ " uoToStatusId:" + upToStatusId);
			}
			// The stream is stalling. Not good.
			@Override
			public void onStallWarning(StallWarning warning) {
				System.out.println("Got stall warning:" + warning);
			}
			// Java has done a bad thing. Everything is terrible.
			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
				System.exit(1);
			}
			
		};
		
		// Set up the connection itself.
		TwitterStreamFactory tsf = new TwitterStreamFactory(cb.build());
		TwitterStream ts = tsf.getInstance();
		// Call the stream listener set up earlier.
		ts.addListener(listener);
		//double[][] YorkBoston = {{-74.0, 40.0}, {-70.6, 42.6}};
		//double[][] newYork = {{-74.0, 40.0}, {-73.0, 41.0}};
		//double[][] NEUSA = {{-77.8, 36.6}, {-70.0, 43.7}};
		double[][] theWorld = {{-180, -90}, {180, 90}};
		//double[][] northAmerica = {{-169, 13}, {-51, 90}};
		// Only retrieve tweets from the given location. 
		FilterQuery filter = new FilterQuery();
		filter.locations(theWorld);
		// Apply the filter and run the stream!
		ts.filter(filter);
	}
	
	// Why does this not exist yet.
	public static <T, E> T getKeysByValue(HashMap<T, E> map, E value) {
		for (Entry<T, E> entry : map.entrySet()) {
			if (Objects.equals(value, entry.getValue())) {
				return entry.getKey();
			}
		}
		return null;
	}
}
