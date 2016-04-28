/*
 * TestTwitterStream12.java
 * 11/4/2016
 * Ezekiel Robertson
 * 
 * Set up a filtered Twitter stream using Twitter4j. Run a lda topic model on
 * the tweets, assigning topic labels to the geotagged English-language tweets.
 * 
 * Variables and file IO are currently hard-coded. 
 * 
 * Now using Tagger.java/Twokenizer.java to tokenize the text data. MalletLDA.java
 * and WorkerRunnable.java run the topic modeler.
 */

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import org.json.simple.JSONObject;
import cc.mallet.types.InstanceList;
import cc.mallet.types.Alphabet;
import cc.mallet.util.Maths;

public class TestTwitterStream12 {
	
	public static void main(String[] args) throws TwitterException, IOException{
		// Configure OAuth keys/tokens for "Zeke's Test Stream" application. If
		// the application is changed in any way on apps.twitter.com, these
		// OAuth keys will also change, and the strings will need to be edited.
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setOAuthConsumerKey("8Nka5YKCKKT2mq53eZV0sN9IW");
		cb.setOAuthConsumerSecret("IgziRUuminrSOdOmwDfqVJ0fhNqeJW2lqaHIU0lNMEtFIx4RGS");
		cb.setOAuthAccessToken("4805189297-SmHEh2MaoKsElGOgEnjKJr7IpV6cPRBZHDEHBEp");
		cb.setOAuthAccessTokenSecret("gZXMZ6DG2vMwOQeJSWXJvbJkcWmruxWbDoJ9Cutnd3qx2");
		// We like having lots of information when something goes wrong, so set
		// these to 'true'.
		cb.setDebugEnabled(true);
		cb.setPrettyDebugEnabled(true);
		// Set the number of times the stream will restart itself. 65536 is
		// entirely arbitrary.
		cb.setHttpRetryCount(65536);
		
		// Full file path to the model used by the part of speech tagger.
		final String MODELNAME = "C:\\Users\\User\\Documents\\Zeke\\model.20120919.txt";
		//Load and initialize the POS tagger. Part of Tagger.java.
		Tagger tagger = new Tagger();
		tagger.loadModel(MODELNAME);
		
		// List of cleaned up tweet strings and their ID numbers to be passed to
		// the Mallet pipes for instancing.
		List<String> cleanList = new ArrayList<String>();
		ArrayList<Long> statusIds = new ArrayList<Long>();
		
		// ArrayList of JSON objects, for storing the data in the order it is
		// received while the LDA is finding topics to assign to each tweet.
		ArrayList<JSONObject> jsonStorage = new ArrayList<JSONObject>();
		
		
		// Stream and process tweets, storing 10000 at a time as strings/JSON
		// for topic modeling.
		StatusListener listener = new StatusListener() {
			// Ratio of information retained from previous runs. Used to
			// calculate priors and the number of tweets carried over from
			// previous runs.
			double c = 0.7;
			// List of indexes for tweets that have been geotagged.
			List<Integer> geoIndexes = new ArrayList<Integer>();
			// We only want so many tweets to be read. Total tweets read is
			// equal to Interval * (1 + c * (Iterations - 1))
			int countTweets = 0;
			int countIterations = 0;
			int topicTrigger = 0;
			int Interval = 100000;
			int Iterations = 50;
			// While copying over json objects, use this to disable tweet intake.
			boolean enabled = true;
			ArrayList<JSONObject> jsonTemp = new ArrayList<JSONObject>();
			// Build the LDA
			int numTopicsMain = 500;
			double alpha0 = 0.01;
			double beta0 = 0.01;
			// For re-calculating beta between LDA runs. An early implementation
			// of the prior calculation from the 2012 online lda paper.
			int oldNumTypesM = -1;
			int oldNumDocsM = -1;
			int oldNumTopicsM = -1;
			int oldNumTokensM = -1;
			int[][] prevTypeTopicCountsM;
			int[][] prevAlphaCountsM;
			// Our mapping of words to integers.
			Alphabet alphabet = null;
			// #CareerArc bots are cluttering up the data. Remove it?
			final boolean REMOVECAREERARC = false; 
			// Set all tokens to lower case to reduce vocabulary size. 
			final boolean lowercase = true;
			// Tags for POS to be removed. See arc-tweet-nlp-0.3.2\docs\annot_guidelines.md
			// for the meanings of each tag.
			final List<String> REMOVE = Arrays.asList(",","@","U","E","G","~");
			// Load the file of stopwords.
			Stream<String> stopStream = 
					Files.lines(Paths.get("C:\\Users\\User\\Documents\\Zeke\\StanfordNlpStopwords.txt"));
			List<String> stopwords = stopStream.collect(Collectors.toList());
			// Set date format for the save file names.
			DateFormat dateFormat = new SimpleDateFormat("ddMMyyy");
			
			// Everything is going well and we just received a new tweet.
			@Override
			public void onStatus(Status status) {
				try {
					// Check for #CareerArc if applicable, as well as if we are
					// temporarily stalling to copy lists.
					if (enabled && !(REMOVECAREERARC && status.getText().contains("#CareerArc"))) {
									
					// Location filter does not always return geotagged tweets.
					// Also, we cannot check language and location at the same
					// time, the location search is an inclusive OR with other
					// arguments.
					if (status.getGeoLocation() != null) {
						// Store geotagged tweets as JSON objects for 
						// visualization.
						JSONObject json = new JSONObject();
						json.put("text", status.getText());
						json.put("latitude", status.getGeoLocation().getLatitude());
						json.put("longitude", status.getGeoLocation().getLongitude());
						json.put("time", status.getCreatedAt().getTime());
						jsonStorage.add(json);
						// Get a list of indexes where these geolocated tweets 
						// live so we can assign them topics.
						geoIndexes.add(topicTrigger);
					}
					
					// Get the text parts
					String text = status.getText();
					// Change to lowercase
					if (lowercase) {
						text = text.toLowerCase();
					}
					
					String tokenLine = new String("");
					statusIds.add(status.getId());
					
					// Tokenize and tag the text strings, saving only the tokens
					// which make it past our lists of invalid POS or stopwords.
					List<Tagger.TaggedToken> taggedTokens = tagger.tokenizeAndTag(text);
					for (Tagger.TaggedToken token : taggedTokens) {
						if ((!REMOVE.contains(token.tag)) && 
								(!stopwords.contains(token.token))) {
							tokenLine += token.token + " ";
						}
					}
					// Add the token string to the list (corpus) of tokenLine
					// (documents)
					cleanList.add(tokenLine);
					
					// This part does nothing but let me know everything is fine
					countTweets++;
					if (countTweets % 100 == 0) {
						System.out.println("Current tweet count: " + countTweets);
					}
					
					// Start up the topic model if either 10000 tweets have been
					// read, or five minutes have passed.
					topicTrigger++;
					if (topicTrigger >= Interval) {
						// Build the file to store tweet data for the lda.
						Date date = new Date();
						File jsonFile = new File("C:\\Users\\User\\Documents\\Zeke\\test_olda\\"
								+ countIterations + dateFormat.format(date) +".txt");
						File topicFile = new File("C:\\Users\\User\\Documents\\Zeke\\topics\\topics"
								+ countIterations + dateFormat.format(date) +".txt");
						File alphabetFile = new File("C:\\Users\\User\\Documents\\Zeke\\alphabet\\alphabet"
								+ countIterations + dateFormat.format(date) +".txt");
						if (!jsonFile.exists()) {
							jsonFile.createNewFile();
						}
						if (!alphabetFile.exists()) {
							alphabetFile.createNewFile();
						}
						FileOutputStream jsonFOS = new FileOutputStream(jsonFile);
						OutputStreamWriter jsonOSW = new OutputStreamWriter(jsonFOS);
						FileOutputStream alphabetFOS = new FileOutputStream(alphabetFile);
						OutputStreamWriter alphabetOSW = new OutputStreamWriter(alphabetFOS);
						countIterations++;
						// Pause the intake so we have time to copy over the
						// jsons to a temporary file, and clean out the buffer.
						enabled = false;
						jsonTemp.addAll(jsonStorage);
						jsonStorage.clear();
						// Reset triggers for the topic modeler;
						topicTrigger = 0;
						//topicTrigger = (int)Math.round(Interval * c);
						
						String[] cleanArray = new String[cleanList.size()];
						cleanArray = cleanList.toArray(cleanArray);
						enabled = true;
						System.out.println("~~~~~~RUN THE TOPIC MODELER~~~~~~");
					
						try {
							// Convert the string array into something Mallet
							// can understand.
							ListToInstance importer = new ListToInstance(statusIds, alphabet);
							InstanceList instances = importer.readArray(cleanArray);
							// Clear out these lists for later.
							statusIds.clear();
							
							MalletLDA lda = new MalletLDA(numTopicsMain, 
									alpha0, beta0, c, oldNumTypesM, oldNumDocsM);
							// If we have a previous run, set priors
							if (oldNumTypesM > 0) {
								//DEBUG
								System.err.println(prevTypeTopicCountsM.length +", "+ 
										prevTypeTopicCountsM[0].length);
								lda.setOldNumTypes(oldNumTypesM);
								lda.setOldNumDocs(oldNumDocsM);
								lda.setPrevAlphaCounts(prevAlphaCountsM);
								lda.setPrevTypeTopicCounts(prevTypeTopicCountsM);
								lda.setOldTokens(oldNumTokensM);
								//DEBUG
								//System.out.println(alphabet.size());
								lda.setAlphabet(alphabet);
							}
							lda.addInstances(instances);
							// MalletLDA.java is multithreaded, so use as many
							// as the computer has available to it.
							lda.setNumThreads(Runtime.getRuntime().availableProcessors());
							// The important bit that does the topic modeling
							lda.estimate();
							//lda.printTopicDocuments(topicFile, 15, cleanArray);
							
							// Get information out for priors
							oldNumTopicsM = lda.getNumTopics();
							oldNumTypesM = lda.getNumTypes();
							oldNumDocsM = lda.getNumDocs();
							oldNumTokensM = lda.getNumTokens();
							//prevAlphaCountsM = new int[oldNumDocsM][oldNumTopicsM];
							//prevTypeTopicCountsM = new int[oldNumTypesM][oldNumTopicsM];
							prevAlphaCountsM = lda.getAlphaCounts();
							prevTypeTopicCountsM = lda.getTypeTopicCounts();
							//DEBUG
							System.err.println(prevTypeTopicCountsM.length +", "+ 
									prevTypeTopicCountsM[0].length);
							alphabet = lda.getAlphabet();
							//DEBUG
							/*
							for (int i = 0; i < alphabet.size(); i++) {
								alphabetOSW.write(alphabet.lookupObject(i).toString() + "\n");
							}
							*/
							//DEBUG
							if (countIterations >= 2) {
								double[] JSD = lda.topicJSD();
								for (int j = 0; j < numTopicsMain; j++) {
									System.out.println(JSD[j]);
								}
							}
							// Assign topics to each tweet stored in JSON ArrayList.
							ArrayList<String> topicArray = lda.getTopicArray(10);
							// Debugging information. The topic and string arrays
							// should be the same size, while the JSON array of
							// geotagged tweets should be 12% of this.
							System.out.println("Topic Array Size: " + topicArray.size());
							System.out.println("JSON Array Size: " + jsonTemp.size());
							System.out.println("String Array Size: " + cleanList.size());
							// Attach the topics, and store the JSON array to file.
							int jj = 0;
							for (JSONObject jType: jsonTemp) {
								if (jType.containsKey("topic")) {
									continue;
								}
								else {
									jType.put("topic", topicArray.get(geoIndexes.get(jj)));
									jsonOSW.write(jType.toString() + "\n");
									jj++;
								}
							}
						}
						catch (Exception e) {
							e.printStackTrace();
						}
						jsonOSW.close();
						jsonFOS.close();
						alphabetOSW.close();
						alphabetFOS.close();
						// Clear out the temporary lists, making sure to keep
						// c% of the previous tweets.
						cleanList.subList(0, cleanList.size() - 
								(int)Math.round(Interval * c)).clear();
						// Print out the labeled tweet output to the console.
						// Timed to put out about 1440 per minute.
						/*
						for (JSONObject jType : jsonTemp) {
							System.out.println(jType.toString());
							try {
								TimeUnit.MILLISECONDS.sleep(250);
							} catch (InterruptedException e) {
								
							}
						}
						*/
						
						jsonTemp.clear();
					}
					// Automatic shut-off after the maximum iterations have been
					// processed.
					if (countIterations >= Iterations) {
						
						
						
						System.exit(0);
					}
					}
					
				} catch (Exception ex){
					ex.printStackTrace();
				}
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
				//System.exit(1);
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
		filter.locations(theWorld).language("en");
		//filter.language(new String[]{"en"});
		// Apply the filter and run the stream!
		ts.filter(filter);
		
	}

}
