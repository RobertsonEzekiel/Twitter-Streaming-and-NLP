/*
 * TestTwitterStream6.java
 * 8/2/2016
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

public class TestTwitterStream6 {
	// Flags for options, consolidated up here.
	// We only want so many tweets to be read.
	static int countTweets = 0;
	static int topicTrigger = 0;
	static int MAXTWEETS = 10000;
	// Set all tokens to lower case to reduce vocabulary size. 
	final static boolean lowercase = true;
	// Full file path to the model used by the POS tagger.
	final static String MODELNAME = "C:\\Users\\User\\workspace\\StreamingWk3\\src\\model.20120919.txt";
	// Tags for POS to be removed. See arc-tweet-nlp-0.3.2\docs\annot_guidelines.md
	// for the meanings of each tag.
	final static List<String> REMOVE = Arrays.asList(",","@","U","E","G","~");
	// Set time for running the topic model
	static long initialTime = System.currentTimeMillis();
	// List of stopwords from Stanford NLP
	static List<String> stopwords = new ArrayList<String>();
	
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
		File topicFile = new File("C:\\Users\\User\\test_olda\\test11022016.txt");
		if (!topicFile.exists()) {
			topicFile.createNewFile();
		}
		File jsonFile = new File("C:\\Users\\User\\test_olda\\json11022016.txt");
		if (!jsonFile.exists()) {
			jsonFile.createNewFile();
		}
		FileOutputStream jsonFOS = new FileOutputStream(jsonFile);
		OutputStreamWriter jsonOSW = new OutputStreamWriter(jsonFOS);
		//FileOutputStream topicFOS = new FileOutputStream(topicFile);
		//OutputStreamWriter topicOSW = new OutputStreamWriter(topicFOS);
		// Load the file of stopwords.
		Stream<String> stream = Files.lines(Paths.get("C:\\Users\\User\\Documents\\Zeke\\StanfordNlpStopwords.txt"));
		stopwords = stream.collect(Collectors.toList());
		
		//Load and initialize the POS tagger. Part of Tagger.java.
		Tagger tagger = new Tagger();
		tagger.loadModel(MODELNAME);
		
		// Build the LDA
		int numTopics = 100;
		double alphaSum = (double)numTopics * 0.001;
		double beta = 0.01;
		MalletLDA lda = new MalletLDA(numTopics, alphaSum, beta);
		
		// List of cleaned up tweet strings to be passed to the Mallet pipes
		// for instancing.
		List<String> cleanList = new ArrayList<String>();
		ArrayList<Long> statusIds = new ArrayList<Long>();
		
		// ArrayList of JSON objects, for storing the data in the order it is
		// received while the LDA is finding topics to assign to each tweet.
		ArrayList<JSONObject> jsonStorage = new ArrayList<JSONObject>();
		
		// Stream, and print statuses from given location to stdout. Nothing is 
		// stored, so there should be no memory problems I think.
		StatusListener listener = new StatusListener() {
			// Everything is going well, print the twitter message, and the name
			// of whoever sent it.
			@Override
			public void onStatus(Status status) {
				try {
					// Debug
					//System.out.println(status.getLang());
					
					// Location filter does not always return geotagged tweets.
					// Also, we cannot check language and location at the same
					// time, the location search is an inclusive OR with other
					// arguments.
					if ((status.getGeoLocation() != null) && (status.getLang().equals("en"))) {
					// JSON object to write to a database for visualization.
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
					if ((topicTrigger >= 10000) && !lda.estimatorRunning) {
						topicTrigger = 0;
						initialTime = System.currentTimeMillis();
						System.out.println("~~~~~~RUN THE TOPIC MODELER~~~~~~");
						String[] cleanArray = new String[cleanList.size()];
						cleanArray = cleanList.toArray(cleanArray);
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
							int index = 0;
							for (JSONObject jType: jsonStorage) {
								jType.put("topic", topicArray.get(index));
								index++;
							}
						}
						catch (Exception e) {
							e.printStackTrace();
						}
						// Print out the json ojbect that will be sent to visualization.
						for (JSONObject jType: jsonStorage) {
							jsonOSW.write(jType.toString() + "\n");
						}
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
						for (int index = 0; index < args.length; index++) {
							System.out.println(args[index]);
						}
						stream.close();
						System.exit(0);
					}
					// Reset the tagger.
					taggedTokens = null;
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
				stream.close();
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
		double[][] northAmerica = {{-169, 13}, {-51, 90}};
		// Only retrieve tweets from the given location. 
		FilterQuery filter = new FilterQuery();
		filter.locations(northAmerica);
		// Cannot use the filter alone; this tries to load too much and Twitter
		// cuts the stream off.
		// ts.sample();
		ts.filter(filter);
		
	}

}
