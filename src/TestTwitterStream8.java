/*
 * TestTwitterStream8.java
 * 24/2/2016
 * Ezekiel Robertson
 * 
 * Set up a basic Twitter stream using Twitter4j. The goal for now is to connect
 * and retrieve tweets from a single user or hashtag. The text will be printed
 * to stdout and saved to a file. After recording 100-100000 tweets, close the stream
 * and exit to avoid accidentally destroying my hard drive.
 * 
 * Now using Tagger.java/Twokenizer.java to tokenize the text data.
 * 
 * I am serious, future me. No reading too much at once.
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
import org.json.simple.JSONObject;
import cc.mallet.types.InstanceList;

public class TestTwitterStream8 {
	// Flags for options, consolidated up here.
	// #CareerArc is cluttering up the data. Remove it?
	final static boolean CAREERARC = true; 
	// We only want so many tweets to be read.
	static int countTweets = 0;
	static int topicTrigger = 0;
	static int Interval = 10000;
	static int MAXTWEETS = Interval * 2;
	// Set all tokens to lower case to reduce vocabulary size. 
	final static boolean lowercase = true;
	// Full file path to the model used by the POS tagger.
	final static String MODELNAME = "C:\\Users\\User\\Documents\\Zeke\\model.20120919.txt";
	// Tags for POS to be removed. See arc-tweet-nlp-0.3.2\docs\annot_guidelines.md
	// for the meanings of each tag.
	final static List<String> REMOVE = Arrays.asList(",","@","U","E","G","~");
	// Set time for running the topic model
	static long initialTime = System.currentTimeMillis();
	// List of stopwords from Stanford NLP
	static List<String> stopwords = new ArrayList<String>();
	// While copying over json objects, use this to disable tweet intake.
	static boolean enabled = true;
	static ArrayList<JSONObject> jsonTemp = new ArrayList<JSONObject>();
	
	public static void main(String[] args) throws TwitterException, IOException{
		// Set maximum tweets from command line
		if (args.length > 1) {
			MAXTWEETS = Integer.parseInt(args[0]);
			System.out.println(MAXTWEETS);
		}
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
		// Build the file to store tweet data for the lda.
		/*
		File textfile = new File("C:\\Users\\User\\test_olda\\input\\20151003.text");
		if (!textfile.exists()) {
			textfile.createNewFile();
		}
		File timefile = new File("C:\\Users\\User\\test_olda\\input\\20151003.time");
		FileOutputStream textfos = new FileOutputStream(textfile);
		OutputStreamWriter textosw = new OutputStreamWriter(textfos);
		if (!timefile.exists()) {
			timefile.createNewFile();
		}
		FileOutputStream timefos = new FileOutputStream(timefile);
		OutputStreamWriter timeosw = new OutputStreamWriter(timefos);
		*/
		File topicFile = new File("C:\\Users\\User\\test_olda\\test26022016.txt");
		if (!topicFile.exists()) {
			topicFile.createNewFile();
		}
		File jsonFile = new File("C:\\Users\\User\\test_olda\\json26022016.txt");
		if (!jsonFile.exists()) {
			jsonFile.createNewFile();
		}
		FileOutputStream jsonFOS = new FileOutputStream(jsonFile);
		OutputStreamWriter jsonOSW = new OutputStreamWriter(jsonFOS);
		//FileOutputStream topicFOS = new FileOutputStream(topicFile);
		//OutputStreamWriter topicOSW = new OutputStreamWriter(topicFOS);
		// Load the file of stopwords.
		Stream<String> stopStream = Files.lines(Paths.get("C:\\Users\\User\\Documents\\Zeke\\StanfordNlpStopwords.txt"));
		stopwords = stopStream.collect(Collectors.toList());
		stopStream.close();
		
		//Load and initialize the POS tagger. Part of Tagger.java.
		Tagger tagger = new Tagger();
		tagger.loadModel(MODELNAME);
		
		// Build the LDA
		int numTopics = 100;
		double alphaSum = (double)numTopics * 0.001;
		double beta = 0.01;

		
		// List of cleaned up tweet strings to be passed to the Mallet pipes
		// for instancing.
		List<String> cleanList = new ArrayList<String>();
		ArrayList<Long> statusIds = new ArrayList<Long>();
		
		// ArrayList of JSON objects, for storing the data in the order it is
		// received while the LDA is finding topics to assign to each tweet.
		ArrayList<JSONObject> jsonStorage = new ArrayList<JSONObject>();
		
		
		// Stream and process tweets, storing 10000 at a time as strings/JSON
		// for topic modeling.
		StatusListener listener = new StatusListener() {
			// Everything is going well, print the twitter message, and the name
			// of whoever sent it.
			@Override
			public void onStatus(Status status) {
				try {
					if (!enabled || (CAREERARC && status.getText().contains("#CareerArc"))) {
					
					// Debug
					//System.out.println(status.getLang());
					
					// Location filter does not always return geotagged tweets.
					// Also, we cannot check language and location at the same
					// time, the location search is an inclusive OR with other
					// arguments.
					if ((status.getGeoLocation() != null) && (status.getLang().equals("en"))) {					// JSON object to write to a database for visualization.
					JSONObject json = new JSONObject();
					// Print out a list of tokens received in the tweet's text.
					String text = status.getText();
					json.put("text", text);
					if (lowercase) {
						text = text.toLowerCase();
					}
					List<Tagger.TaggedToken> taggedTokens = tagger.tokenizeAndTag(text);
					// Output the tweet in a manner the online lda can recognize
					// System.out.print(status.getId() + "\t");
					// textosw.write(status.getId() + "\t");
					//boolean JSONflag = true;
					// For information to pass on to the instancer.
					String tokenLine = new String("");
					statusIds.add(status.getId());
					for (Tagger.TaggedToken token : taggedTokens) {
						if ((!REMOVE.contains(token.tag)) && (!stopwords.contains(token.token))) {
							//System.out.print(token.token + " ");
							// textosw.write(token.token + " ");
							tokenLine += token.token + " ";
							/*if (JSONflag) {
								json.put("topic", token.token);
								JSONflag = false;
							}*/
						}
					}
					// Add the token string to the list (corpus) of tokenLine
					// (documents)
					cleanList.add(tokenLine);
					// Add remaining fields to the json database object.
					json.put("latitude", status.getGeoLocation().getLatitude());
					json.put("longitude", status.getGeoLocation().getLongitude());
					json.put("time", status.getCreatedAt().getTime());
					// Save the json object into the array for later.
					jsonStorage.add(json);
					//System.out.print("\n");
					// textosw.write("\n");
					// timeosw.write(status.getCreatedAt().toString() + "\n");
					
					// Print out the username, and the text of the tweet.
					//System.out.println("@" + status.getUser().getScreenName() + " - "
					//		+ status.getCreatedAt() + " - " + status.getText());
					
					countTweets++;
					if (countTweets % 100 == 0) {
						System.out.println("Current tweet count: " + countTweets);
					}
					// Start up the topic model if either 10000 tweets have been
					// read, or five minutes have passed.
					topicTrigger++;
					//if (((topicTrigger >= 10000) || (System.currentTimeMillis() 
					//		- initialTime > 300000)) && !lda.estimatorRunning) {
					if (topicTrigger >= Interval) {
						// Pause the intake so we have time to copy over the
						// jsons to a temporary file, and clean out the buffer.
						enabled = false;
						jsonTemp.addAll(jsonStorage);
						jsonStorage.clear();
						// Reset triggers for the topic modeler;
						topicTrigger = 0;
						initialTime = System.currentTimeMillis();
						String[] cleanArray = new String[cleanList.size()];
						cleanArray = cleanList.toArray(cleanArray);
						enabled = true;
						System.out.println("~~~~~~RUN THE TOPIC MODELER~~~~~~");
						
						/*for (int index = 0; index < cleanList.size(); index++) {
							System.out.println(cleanArray[index]);
							System.out.println(cleanList.get(index));
						}*/
						
						try {
							// Convert the string array into something Mallet
							// Can understand.
							ListToInstance importer = new ListToInstance(statusIds);
							InstanceList instances = importer.readArray(cleanArray);
							// Clear out these lists for later.
							statusIds.clear();
							cleanList.clear();
							MalletLDA lda = new MalletLDA(numTopics, alphaSum, beta);
							lda.addInstances(instances);
							// MalletLDA.java is multithreaded, so use as many
							// as the computer has available to it.
							lda.setNumThreads(Runtime.getRuntime().availableProcessors());
							lda.estimate();
							// DEBUG: print the topics with their documents to
							// make sure the lda isn't doing anything stupid.
							lda.printTopWord(topicFile);
							ArrayList<String> topicArray = lda.getTopicArray();
							// Assign topics to each tweet stored in JSON ArrayList.
							System.out.println("Topic Array Size: " + topicArray.size());
							System.out.println("JSON Array Size: " + jsonTemp.size());
							int jj = 0;
							for (JSONObject jType: jsonTemp) {
								if (jType.containsKey("topic")) {
									continue;
								}
								else {
									jType.put("topic", topicArray.get(jj));
									jsonOSW.write(jType.toString() + "\n");
									jj++;
								}
							}
						}
						catch (Exception e) {
							e.printStackTrace();
						}
						jsonTemp.subList(0, jsonTemp.size() - (int)Math.round(Interval * .4)).clear();
					}
					// Automatic shut-off after MAXTWEETS have been processed.
					if (countTweets >= MAXTWEETS) {
						/*
						textosw.close();
						textfos.close();
						timeosw.close();
						timefos.close();
						*/
						//topicOSW.close();
						//topicFOS.close();
						jsonOSW.close();
						jsonFOS.close();
						// DEBUG: Check if command line arguments are working.
						for (int ii = 0; ii < args.length; ii++) {
							System.out.println(args[ii]);
						}
						System.exit(0);
					}
					// Reset the tagger.
					taggedTokens = null;
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
		//double[][] theWorld = {{-180, -90}, {180, 90}};
		double[][] northAmerica = {{-169, 13}, {-51, 90}};
		// Only retrieve tweets from the given location. 
		FilterQuery filter = new FilterQuery();
		filter.locations(northAmerica);
		//filter.language(new String[]{"en"});
		// Apply the filter and run the stream!
		ts.filter(filter);
		
	}

}
