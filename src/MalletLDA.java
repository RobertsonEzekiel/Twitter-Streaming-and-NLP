/*
 * MalletLDA.java
 * 1/2/2016
 * Ezekiel Robertson
 * 
 * LDA made by combining LDAStream and ParallelTopicModel from Mallet. This
 * should result in a multithreaded, live, online LDA.
 */

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
//import java.util.List;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Formatter;
//import java.util.Locale;
//import java.util.zip.*;
import java.util.concurrent.*;
import java.util.logging.*;
import java.text.NumberFormat;

import cc.mallet.types.Alphabet;
import cc.mallet.types.LabelAlphabet;
import cc.mallet.types.InstanceList;
import cc.mallet.types.Instance;
import cc.mallet.types.LabelSequence;
//import cc.mallet.types.Dirichlet;
import cc.mallet.types.IDSorter;
import cc.mallet.types.FeatureSequence;
//import cc.mallet.types.FeatureVector;
import cc.mallet.topics.TopicAssignment;
import cc.mallet.util.Randoms;
import cc.mallet.util.MalletLogger;
import cc.mallet.util.Maths;

//import gnu.trove.*;

public class MalletLDA implements Serializable{
	public static final int UNASSIGNED_TOPIC = -1;
	
	public static Logger logger = MalletLogger.getLogger(MalletLDA.class.getName());
	
	public ArrayList<TopicAssignment> data;
	public Alphabet alphabet;
	public LabelAlphabet topicAlphabet;
	public int numTopics;
	public int topicMask;
	public int topicBits;
	public int numTypes;
	public int totalTokens;
	public double alpha0;
	public double[][] alpha;
	public double alphaSum;
	public double beta0;
	public double[][] beta;
	public double betaSum;
	public int[][] prevTypeTopicCounts;
	//public int[][] threadCounts;
	public double c;
	public int oldNumTypes = -1;
	public int oldNumDocs = -1;
	public int oldTokens = -1;
	public int newTypes;
	public int[][] alphaCounts;
	public int[][] prevAlphaCounts;
	public boolean usingSymmetricAlpha = false;
	public static final double DEFAULT_BETA = 0.01;
	public int[][] typeTopicCounts;
	public int[] tokensPerTopic;
	public int[] docLengthCounts;
	public double[][] topicDocCountsHere;
	public int numIterations = 1000;
	public int burninPeriod = 200;
	public int saveSampleInterval = 10;
	public int optimizeInterval = 50;
	public int temperingInterval = 0;
	public int showTopicsInterval = 50;
	public int wordsPerTopic = 7;
	public int saveStateInterval = 0;
	public String stateFilename = null;
	public int saveModelInterval = 0;
	public String modelFilename = null;
	public int randomSeed = -1;
	public NumberFormat formatter;
	public boolean printLogLikelihood = true;
	public boolean estimatorRunning = false;
	int[] typeTotals;
	int maxTypeCount;
	int numThreads = 1;
	
	public static LabelAlphabet newLabelAlphabet (int numTopics) {
		LabelAlphabet ret = new LabelAlphabet();
		for (int i = 0; i < numTopics; i++) {
			ret.lookupIndex("topic"+i);
		}
		return ret;
	}
	
	public MalletLDA (int numberOfTopics, double alphaSum, double beta, double c, int oldNumTypes, int oldNumDocs) {
		this (newLabelAlphabet(numberOfTopics), alphaSum, beta, c, oldNumTypes, oldNumDocs);
	}
	
	public MalletLDA (LabelAlphabet topicAlphabet, double alpha0, double beta0,
				double c, int oldNumTypes, int oldNumDocs) {
		this.data = new ArrayList<TopicAssignment>();
		this.topicAlphabet = topicAlphabet;
		this.numTopics = topicAlphabet.size();
		this.c = c;
		this.oldNumTypes = oldNumTypes;
		this.oldNumDocs = oldNumDocs;
		
		// Set topicBits as an exact power of 2
		if (Integer.bitCount(numTopics) == 2) {
			// Exact power of 2
			topicMask = numTopics - 1;
			topicBits = Integer.bitCount(topicMask);
		}
		else {
			// Otherwise add an extra bit
			topicMask = Integer.highestOneBit(numTopics) * 2 - 1;
			topicBits = Integer.bitCount(topicMask);
		}
		
		
		
		this.alpha0 = alpha0;
		this.beta0 = beta0;
		
		if (oldNumTypes > 0) {
			this.prevTypeTopicCounts = new int[oldNumTypes][numTopics];
			this.prevAlphaCounts = new int[oldNumDocs][numTopics];
		}
		
		tokensPerTopic = new int[numTopics];
		
		formatter = NumberFormat.getInstance();
		formatter.setMaximumFractionDigits(5);
		
		logger.info("Coded LDA: " + numTopics + " topics, " + topicBits +
				" topic bits, " + Integer.toBinaryString(topicMask) + 
				"topicMask");
	}
	
	// addInstances is the part which loads a new corpus file as an InstanceList
	// into the LDA, preparing it for topic modeling.
	public void addInstances (InstanceList training) {
		
		int oldAlphabet = 0;
		if (oldNumTypes > 0) {
			
			oldAlphabet = alphabet.size();
			Alphabet tempAlphabet;
			tempAlphabet = training.getDataAlphabet();
			newTypes = tempAlphabet.size();
			System.out.println(newTypes);
			alphabet.lookupIndices(training.getDataAlphabet().toArray(), true);
		} else {
			alphabet = training.getDataAlphabet();
			newTypes = alphabet.size();
		}
		//DEBUG
		System.out.println("New alphabet size: "+alphabet.size());
		numTypes = alphabet.size();
		
		if (oldNumTypes > 0) {
			if (numTypes > oldAlphabet) {
				System.out.println("Our vocabulary is expanding!");
			}
			
		}
		alphaCounts = new int[training.size()][numTopics];
		topicDocCountsHere = new double[numTopics][training.size()];
		
		
		//DEBUG
		//System.out.println("Is beta 0? " + beta[0][0]);
		
		typeTopicCounts = new int[numTypes][];
		typeTotals = new int[numTypes];
		
		for (Instance instance : training) {
			FeatureSequence tokens = (FeatureSequence)instance.getData();
			for (int position = 0; position < tokens.getLength(); position++) {
				int type = tokens.getIndexAtPosition(position);
				typeTotals[type]++;
			}
		}
		
		maxTypeCount = 0;
		for (int type = 0; type < numTypes; type++) {
			if (typeTotals[type] > maxTypeCount) {
				maxTypeCount = typeTotals[type];
			}
			typeTopicCounts[type] = new int [Math.min(numTopics, 
					typeTotals[type])];
		}
		
		// Set up the PRNG, checking if someone entered a custom random seed
		Randoms random = null;
		if (randomSeed == -1) {
			random = new Randoms();
		}
		else {
			random = new Randoms(randomSeed);
		}
		
		for (Instance instance : training) {
			FeatureSequence tokens = (FeatureSequence)instance.getData();
			LabelSequence topicSequence = new LabelSequence(topicAlphabet, new 
					int [tokens.size()]);
			int[] topics = topicSequence.getFeatures();
			// Randomize the topics' starting words
			for (int position = 0; position < topics.length; position++) {
				
				int topic = random.nextInt(numTopics);
				topics[position] = topic;
			}
			// Add this new information onto the larger data file
			TopicAssignment t = new TopicAssignment(instance, topicSequence);
			data.add(t);
		}
		// Complete the initializaiton process
		buildInitialTypeTopicCounts();
		initializeHistograms();
	}
	// Initialize the type-topic counts. Note that this will require lots of
	// modification, in order to keep some of a previous run's data.
	// TODO: Edit to allow some data from previous runs to stay 
	private void buildInitialTypeTopicCounts() {
		// Clear the topic totals
		Arrays.fill(tokensPerTopic, 0);
		// Clear the type/topic counts, only looking at the entries before the
		// first 0 entry.
		for (int type = 0; type < numTypes; type++) {
			int[] topicCounts = typeTopicCounts[type];
			if (topicCounts.length == 0) {
				continue;
			}
			int position = 0;
			while ((position < typeTopicCounts.length) && 
					(topicCounts[position]> 0)) {
				topicCounts[position] = 0;
				position++;
			}
		}
		
		int doc = 0;
		
		for (TopicAssignment document : data) {
			FeatureSequence tokens = (FeatureSequence)document.instance.getData();
			LabelSequence topicSequence = (LabelSequence)document.topicSequence;
			int[] topics = topicSequence.getFeatures();
			
			for (int position = 0; position < tokens.size(); position++) {
				int topic = topics[position];
				
				if (topic == UNASSIGNED_TOPIC) {
					continue;
				}
				tokensPerTopic[topic]++;
				alphaCounts[doc][topic]++;
				topicDocCountsHere[topic][doc]++;
				
				// The format for these arrays is: The topic in the rightmost
				// bits; the count in the remaining (left) bits. Since the count
				// is in the high bits, sorting (descending) by the numeric
				// value of the int guarantees that higher counts will be before
				// the lower counts.
				int type = tokens.getIndexAtPosition(position);
				int[] currentTypeTopicCounts = typeTopicCounts[type];
				
				// Start by assuming the array is either empty or is sorted in
				// descending order. Here we are only adding counts, so if we
				// find an existing location with the topic, we only need to
				// ensure that it is not larger than its left neighbor.
				int index = 0;
				int currentTopic = currentTypeTopicCounts[index] & topicMask;
				int currentValue;
				// DEBUG:
				//System.out.println(currentTypeTopicCounts.length);
				try {
				while ((currentTypeTopicCounts[index] > 0) && (currentTopic != 
						topic)) {
					index++;
					
					if (index == currentTypeTopicCounts.length) {
						logger.info("overflow on type " + type);
					}
					currentTopic = currentTypeTopicCounts[index] & topicMask;
					
				}
				} catch (IndexOutOfBoundsException e) {
					continue;
				}
				currentValue = currentTypeTopicCounts[index] >> topicBits;
		
				if (currentValue == 0) {
					// The new value is 1, so we don't have to worry about 
					// sorting (except by topic suffix, which doesn't matter).
					currentTypeTopicCounts[index] = (1 << topicBits) + topic;
				}
				else {
					currentTypeTopicCounts[index] = ((currentValue + 1) << 
							topicBits) + topic;
					// Now ensure that the array is still sorted by bubbling
					// this value up.
					while ((index > 0) && (currentTypeTopicCounts[index] > 
							currentTypeTopicCounts[index - 1])) {
						int temp = currentTypeTopicCounts[index];
						currentTypeTopicCounts[index] = 
								currentTypeTopicCounts[index - 1];
						currentTypeTopicCounts[index - 1] = temp;
						
						index--;
					}
				}
			}
			doc++;
		}
	}
	
	// Calculate the size of documents to create histograms for use in 
	// "Dirichlet hyperparameter optimization." 
	private void initializeHistograms() {
		int maxTokens = 0;
		totalTokens = 0;
		int seqLen;
		// Count the number of tokens in the corpus, and the maximum size of a 
		// document.
		for (int doc = 0; doc < data.size(); doc++) {
			FeatureSequence fs = (FeatureSequence)data.get(doc).instance.getData();
			seqLen = fs.getLength();
			if (seqLen > maxTokens) {
				maxTokens = seqLen;
			}
			totalTokens += seqLen;
		}
		logger.info("max tokens: " + maxTokens);
		logger.info("total tokens: " + totalTokens);
		
		docLengthCounts = new int[maxTokens + 1];
		topicDocCountsHere = new double[numTopics][data.size()];
		// Calculate alpha, beta based on the equation from "Online Trend Analysis With
		// Topic Models, section 3.2.
		//TODO bookmark this
		alpha = new double[data.size()][numTopics];
		for (double[] row : alpha) {
			Arrays.fill(row, alpha0);
		}
		beta = new double[numTopics][numTypes];
		for (double[] row : beta) {
			Arrays.fill(row, beta0);
		}
		//System.out.println("Beta Size: " + beta.length + " x " + beta[0].length);
		//System.out.println("Alpha Size: " + alpha.length + " x " + alpha[0].length);
		
		if (oldNumTypes > 0) {
			//DEBUG
			//System.err.println(prevTypeTopicCounts.length +", "+ 
			//		prevTypeTopicCounts[0].length);
			//System.out.println("TTC Size: " + prevTypeTopicCounts.length + " x " + prevTypeTopicCounts[0].length);
			//System.out.println("AC Size: " + prevAlphaCounts.length + " x " + prevAlphaCounts[0].length);
			int oldTokensB = 0;
			for (int w = 0; w < prevTypeTopicCounts.length; w++) {
				for (int t = 0; t < prevTypeTopicCounts[w].length; t++) {
					oldTokensB += prevTypeTopicCounts[w][t];
				}
			}
			for (int type = 0; (type < prevTypeTopicCounts.length && type < numTypes); type++) {
				//for (int t = 0; (t < prevTypeTopicCounts[w].length && t < numTopics); t++) {
				
				int[] topicCounts = prevTypeTopicCounts[type];
				
				int index = 0;
				while ((index < topicCounts.length) && (topicCounts[index] > 0)) {
					int topic = topicCounts[index] & topicMask;
					int count = topicCounts[index] >> topicBits;
					if (count > 0) {
						beta[topic][type] = beta0*(1.0-c) + (double)count * (double)numTopics
								* (double)newTypes * beta0 * c / (double)oldTokensB;
					}
					
					//DEBUG
					//System.out.println(beta[topic][type]);
										
					index++;
				}
						
					//}
				//System.out.println();
				//DEBUG
				//System.out.println(beta[t][w]);
			}
			try{ 
				TimeUnit.MILLISECONDS.sleep(2000);
			} catch (InterruptedException e) {
			
			}
			int oldTokensA = 0;
			for (int w = 0; w < prevAlphaCounts.length; w++) {
				for (int t = 0; t < prevAlphaCounts[w].length; t++) {
					oldTokensA += prevAlphaCounts[w][t];
				}
			}
			for (int d = 0; d < prevAlphaCounts.length && d < data.size(); d++) {
				for (int t = 0; (t < prevAlphaCounts[d].length && t < numTopics); t++) {
					if (prevAlphaCounts[d][t] > 0) {
						alpha[d][t] = (double)prevAlphaCounts[d][t] * (double)numTopics
								* (double)oldNumDocs * alpha0 / (double)oldTokensA;
						
						//alpha[d][t] *= (double)numTopics;
						//System.out.print(prevAlphaCounts[d][t] + " ");
						//System.out.print(alpha[d][t]);
					}
					//DEBUG
					if (alpha[d][t] < 0) {
						System.out.println("Alpha < 0, " + alpha[d][t]);
					}
				}
				//System.out.println();
			}
		}
		alphaSum = 0;
		for (double[] a : alpha) {
			for (double b : a) {
				alphaSum += b;
			}
		}
		
		alphaSum /= data.size();
		System.out.println("alphaSum: " + alphaSum);
		betaSum = 0;
		for (double[] i : beta) {
			for (double j : i) {
				betaSum += j;
			}
		}
		betaSum /= numTopics;
		/*
		if (oldNumDocs > 0) {
			betaSum /= newTypes;
		} else {
			betaSum /= numTypes;
		}
		*/
		System.out.println("betaSum: " + betaSum);
	}
	/*
	public void optimizeAlpha(WorkerRunnable[] runnables) {
		// First clear out the sufficient statistic histograms
		Arrays.fill(docLengthCounts, 0);
		for (int topic = 0; topic < topicDocCounts.length; topic++) {
			Arrays.fill(topicDocCounts[topic], 0);
		}
		
		for (int thread = 0; thread < numThreads; thread++) {
			int[] sourceLengthCounts = runnables[thread].getDocLengthCounts();
			int[][] sourceTopicCounts = runnables[thread].getTopicDocCounts();
			
			for (int count = 0; count < sourceLengthCounts.length; count++) {
				if (sourceLengthCounts[count] > 0) {
					docLengthCounts[count] += sourceLengthCounts[count];
					sourceLengthCounts[count] = 0;
				}
			}
			
			for (int topic = 0; topic < numTopics; topic++) {
				if (! usingSymmetricAlpha) {
					for (int count = 0; count < sourceTopicCounts[topic].length;
							count++) {
						if (sourceTopicCounts[topic][count] > 0) {
							topicDocCounts[topic][count] += sourceTopicCounts[topic][count];
							sourceTopicCounts[topic][count] = 0;
						}
					}
				}
				else {
					// For the symmetric version, we need only one count array, 
					// which I'm putting in the same data structure, but for
					// topic 0. All other topic histograms will be empty. I'm
					// duplicating this for loop, which isn't the best thing,
					// but it means only checking whether we are symmetric or
					// not numTopics times, instead of numTopics * longest
					// document length.
					for (int count = 0; count < sourceTopicCounts[topic].length;
							count++) {
						if (sourceTopicCounts[topic][count] > 0) {
							topicDocCounts[0][count] += sourceTopicCounts[topic][count];
							sourceTopicCounts[topic][count] = 0;
						}
					}
				}
			}
		}
		
		if (usingSymmetricAlpha) {
			alphaSum = Dirichlet.learnSymmetricConcentration(topicDocCounts[0],
					docLengthCounts, numTopics, alphaSum);
			for (int topic = 0; topic < numTopics; topic++) {
				alpha[topic] = alphaSum / numTopics;
			}
		}
		else {
			alphaSum = Dirichlet.learnParameters(alpha, topicDocCounts,
					docLengthCounts, 1.001, 1.0, 1);
		}
	}
	
	public void optimizeBeta(WorkerRunnable[] runnables) {
		// This histogram starts at count 0, so if all of the tokens of the most
		// frequent type were assigned to one topic, we would need to store a 
		// maxTypeCount + 1 count.
		int[] countHistogram = new int[maxTypeCount + 1];
		// Now count the number of type/topic pairs that have each number of
		// tokens.
		
		int index;
		for (int type = 0; type < numTypes; type++) {
			int[] counts = typeTopicCounts[type];
			
			index = 0;
			while ((index < counts.length) && (counts[index] > 0)) {
				int count = counts[index] >> topicBits;
				countHistogram[count]++;
				index++;
			}
		}
		
		// Figure out how large we need to make the observation lengths 
		// histogram.
		int maxTopicSize = 0;
		for (int topic = 0; topic < numTopics; topic++) {
			if (tokensPerTopic[topic] > maxTopicSize) {
				maxTopicSize = tokensPerTopic[topic];
			}
		}
		
		// Now allocate it and populate it.
		int[] topicSizeHistogram = new int[maxTopicSize + 1];
		for (int topic = 0; topic < numTopics; topic++) {
			topicSizeHistogram[tokensPerTopic[topic]]++;
		}
		
		betaSum = Dirichlet.learnSymmetricConcentration(countHistogram,
				topicSizeHistogram, numTypes, betaSum);
		beta = betaSum / numTypes;
		if (beta < 0.001 || Double.isNaN(beta)) {
			beta = 0.001;
		}
		
		
		logger.info("[beta: " + beta + "] ");
		//logger.info("[beta: " + formatter.format(beta) + "] ");
		
		// Now publish the new beta value
		for (int thread = 0; thread < numThreads; thread++) {
			runnables[thread].resetBeta(beta, betaSum);
		}
	}
	*/
	// Estimate is where the main loops for working through the lda algorithm
	// happen. Gets called from main(), and will require several more methods
	// to function.
	public void estimate() throws IOException {
		long startTime = System.currentTimeMillis();
		estimatorRunning = false;
		// Set up multithreading
		WorkerRunnable[] runnables = new WorkerRunnable[numThreads];
		
		int docsPerThread = data.size() / numThreads;
		int offset = 0;
		// If multithreading,
		if (numThreads > 1) {
			for (int thread = 0; thread < numThreads; thread++) {
				int[] runnableTotals = new int[numTopics];
				System.arraycopy(tokensPerTopic, 0, runnableTotals, 0, 
						numTopics);
				int[][] runnableCounts = new int [numTypes][];
				
				for (int type = 0; type < numTypes; type++) {
					int[] counts = new int[typeTopicCounts[type].length];
					System.arraycopy(typeTopicCounts[type], 0, counts, 0,
							counts.length);
					runnableCounts[type] = counts;
				}
				
				// Some docs may be missing at the end due to integer division.
				if (thread == numThreads - 1) {
					docsPerThread = data.size() - offset;
				}
				// Reset the PRNG
				Randoms random = null;
				if (randomSeed == -1) {
					random = new Randoms();
				}
				else {
					random = new Randoms(randomSeed);
				}
				// Engage threads
				runnables[thread] = new WorkerRunnable(numTopics, alpha, 
						alphaSum, beta, betaSum, newTypes, oldNumDocs, random, data, runnableCounts, 
						runnableTotals, offset, docsPerThread);
				runnables[thread].initializeAlphaStatistics(docsPerThread);
				offset += docsPerThread;
			}
		}
		else {
			// There is only one thread, copy the typeTopicCounts arrays
			// directly, rather than allocating new memory.
			
			// Reset the PRNG
			Randoms random = null;
			if (randomSeed == -1) {
				random = new Randoms();
			}
			else {
				random = new Randoms(randomSeed);
			}
			
			runnables[0] = new WorkerRunnable(numTopics, alpha, 
					alphaSum, beta, betaSum, newTypes, oldNumDocs, random, data, typeTopicCounts, 
					tokensPerTopic, offset, docsPerThread);
			runnables[0].initializeAlphaStatistics(docLengthCounts.length);
			
			// If there is only one thread, we can avoid communications
			// overhead. This switch informs the thread not to gather statistics
			// for its portion of the data.
			runnables[0].makeOnlyThread();
		}
		
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		// Begin the main loop
		for (int iteration = 1; iteration <= numIterations; iteration++) {
			long iterationStart = System.currentTimeMillis();
			// Save information every so often
			/*
			if ((showTopicsInterval != 0) && (iteration != 0) && (iteration % 
					showTopicsInterval == 0)) {
				logger.info("\n" + displayTopWords(wordsPerTopic, false));
			}
			if ((saveStateInterval != 0) && (iteration % 
					saveStateInterval == 0)) {
				this.printState(new File(stateFilename + "." + iteration));
			}
			if ((saveModelInterval != 0) && (iteration % 
					saveModelInterval == 0)) {
				this.write(new File(modelFilename + "." + iteration));
			}
			*/
			if (numThreads > 0) {
				// Submit runnables to thread pool
				for (int thread = 0; thread < numThreads; thread++) {
					if ((iteration > burninPeriod) && (optimizeInterval != 0) &&
							(iteration % saveSampleInterval == 0)) {
						runnables[thread].collectAlphaStatistics();
					}
					
					logger.fine("submitting thread " + thread);
					executor.submit(runnables[thread]);
				}
				
				// The original Mallet coders note that they are getting some
				// problems that look like a thread hasn't started yet when it
				// is first polled, so it appears to be finished. This only
				// happens in very short corpora, which should not be a problem
				// with the Twitter stream.
				try {
					Thread.sleep(20);
				}
				catch (InterruptedException e) {
					
				}
				
				boolean finished = false;
				while (! finished) {
					try {
						Thread.sleep(10);
					}
					catch (InterruptedException e) {
						
					}
					
					finished = true;
					// Are all of the threads done yet?
					for (int thread = 0; thread < numThreads; thread++) {
						finished = (finished) && (runnables[thread].getIsFinished());
					}
				}
				
				sumTypeTopicCounts(runnables);
				
				for (int thread = 0; thread < numThreads; thread++) {
					int[] runnableTotals = runnables[thread].getTokensPerTopic();
					System.arraycopy(tokensPerTopic, 0, runnableTotals, 0, numTopics);
					int[][] runnableCounts = runnables[thread].getTypeTopicCounts();
					
					for (int type = 0; type < numTypes; type++) {
						int[] targetCounts = runnableCounts[type];
						int[] sourceCounts = typeTopicCounts[type];
						
						int index = 0;
						while (index < sourceCounts.length) {
							if (sourceCounts[index] != 0) {
								targetCounts[index] = sourceCounts[index];
							}
							else if (targetCounts[index] != 0) {
								targetCounts[index] = 0;
							}
							else {
								break;
							}
							
							index++;
						}
					}
				}
			}
			// If single thread
			else {
				if ((iteration > burninPeriod) && (optimizeInterval != 0) &&
						(iteration % saveSampleInterval == 0)) {
					runnables[0].collectAlphaStatistics();
				}
				runnables[0].run();
			}
			// Record how long that just took
			long elapsedMillis = System.currentTimeMillis() - iterationStart;
			if (elapsedMillis < 1000) {
				logger.fine(elapsedMillis + "ms ");
			}
			else {
				logger.fine((elapsedMillis / 1000) + "s ");
			}
			// Run the optimizer for alpha and beta every so often
			/*
			if ((iteration > burninPeriod) && (optimizeInterval != 0) &&
					(iteration % optimizeInterval == 0)) {
				optimizeAlpha(runnables);
				optimizeBeta(runnables);
				logger.fine("[O " + (System.currentTimeMillis() - 
						iterationStart) + "] ");
			}
			*/
			/*
			if (iteration % 10 == 0) {
				if (printLogLikelihood) {
					logger.info("<" + iteration + "> LL/token: " + 
				formatter.format(modelLogLiklihood() / totalTokens));
				}
				else {
					logger.info("<" + iteration + ">");
				}
			}
			*/
		}
		// Iteration finished, close the threads and print the elapsed time.
		executor.shutdownNow();
		
		long seconds = Math.round((System.currentTimeMillis() - startTime) / 
				1000.0);
		long minutes = seconds / 60; seconds %= 60;
		// Just in case, copied from the original. I hope these never see use.
		long hours = minutes / 60; minutes %= 60;
		long days = hours / 24; hours %= 24;
		
		StringBuilder timeReport = new StringBuilder();
		timeReport.append("\nTotal time: ");
		if (days != 0) {
			timeReport.append(days); timeReport.append(" days ");
		}
		if (hours != 0) {
			timeReport.append(hours); timeReport.append(" hours ");
		}
		if (minutes != 0) {
			timeReport.append(minutes); timeReport.append(" minutes ");
		}
		timeReport.append(seconds); timeReport.append(" seconds");
		
		logger.info(timeReport.toString());
		estimatorRunning = false;
	}
	
	// Serializaiton
	private static final long serialVersionUID = 1;
	//private static final int CURRENT_SERIAL_VERSION = 0;
	//private static final int NULL_INTEGER = -1;
	
	// Retrieve the top words from a topic and return them and their weights as 
	// a tab (and possibly newline) delimited string.
	// TODO: Edit the outputs to a single word per topic.
	public String displayTopWords(int numWords, boolean usingNewLines) {
		StringBuilder out = new StringBuilder();
		ArrayList<TreeSet<IDSorter>> topicSortedWords = getSortedWords();
		
		// Print the results for each topic.
		for (int topic = 0; topic < numTopics; topic++) {
			TreeSet<IDSorter> sortedWords = topicSortedWords.get(topic);
			int word = 1;
			Iterator<IDSorter> iterator = sortedWords.iterator();
			
			if (usingNewLines) {
				out.append(topic + "\t" + formatter.format(alpha[topic]) + "\n");
				while ((iterator.hasNext()) && (word < numWords)) {
					IDSorter info = iterator.next();
					out.append(alphabet.lookupObject(info.getID()) + "\t" +
							formatter.format(info.getWeight()) + "\n");
					word++;
				}
			}
			else {
				out.append(topic + "\t" + formatter.format(alpha[topic]) + "\t");
				while ((iterator.hasNext()) && (word < numWords)) {
					IDSorter info = iterator.next();
					out.append(alphabet.lookupObject(info.getID()) + "\t" +
							formatter.format(info.getWeight()) + " ");
					word++;
				}
				out.append("\n");
			}
		}
		return out.toString();
	}
	// Return an array of sorted sets, one per topic. Each set contains IDSorter
	// objects with integer keys to the alphabet.
	public ArrayList<TreeSet<IDSorter>> getSortedWords() {
		ArrayList<TreeSet<IDSorter>> topicSortedWords = 
				new ArrayList<TreeSet<IDSorter>>(numTopics);
		// Initialize the tree sets
		for (int topic = 0; topic < numTopics; topic++) {
			topicSortedWords.add(new TreeSet<IDSorter>());
		}
		// Collect counts
		for (int type = 0; type < numTypes; type++) {
			int[] topicCounts = typeTopicCounts[type];
			
			int index = 0;
			while ((index < topicCounts.length) && (topicCounts[index] > 0)) {
				int topic = topicCounts[index] & topicMask;
				int count = topicCounts[index] >> topicBits;
				topicSortedWords.get(topic).add(new IDSorter(type, count));
				
				index++;
			}
		}
		return topicSortedWords;
	}
	
	// Once the runnables are finished their tasks, run through the threads and
	// add the counts from each thread into this class's count arrays.
	public void sumTypeTopicCounts (WorkerRunnable[] runnables) {
		// Clear topic totals
		Arrays.fill(tokensPerTopic, 0);
		
		for (int doc = 0; doc < data.size(); doc++) {
			Arrays.fill(alphaCounts[doc], 0);
		}
		
		// Clear the type/topic counts, only looking at the entries before the
		// first 0 entry.
		for (int type = 0; type < numTypes; type++) {
			int[] targetCounts = typeTopicCounts[type];
			
			int position = 0;
			while ((position < targetCounts.length) && 
					(targetCounts[position] > 0)) {
				targetCounts[position] = 0;
				position++;
			}
		}
		
		// Iterate through all of the WorkerRunnable threads.
		for (int thread = 0; thread < numThreads; thread++) {
			// Handle the total tokens per topic array
			int[] sourceTotals = runnables[thread].getTokensPerTopic();
			for (int topic = 0; topic < numTopics; topic++) {
				tokensPerTopic[topic] += sourceTotals[topic];
			}
			//DEBUG
			//System.out.println("StartDoc: " + runnables[thread].startDoc);
			//System.out.println("NumDocs: " + runnables[thread].numDocs);
			//threadCounts = new int[numTopics][runnables[thread].numDocs];
			//threadCounts = runnables[thread].getTopicDocCounts();
			int[][] threadCounts = runnables[thread].getTopicDocCounts();
			//System.out.println(threadCounts[0][0]);
			for (int topic = 0; topic < numTopics; topic++) {
				for (int doc = 0;
					 	doc < data.size() && doc < runnables[thread].numDocs; doc++) {
					//System.out.println("Local size: " + topicDocCounts[topic].length
					//		+ ", Worker size: " + runnables[thread].topicDocCounts[topic].length);
					//System.out.println(runnables[thread].topicDocCounts[topic][doc]);
					topicDocCountsHere[topic][doc + runnables[thread].startDoc] += 
							threadCounts[topic][doc];
					alphaCounts[doc][topic] += threadCounts[topic][doc];
					//System.out.println(topicDocCountsHere[topic][doc]);
				}
			}
			
			// Now handle the individual type topic counts
			int[][] sourceTypeTopicCounts = runnables[thread].getTypeTopicCounts();
			for (int type = 0; type < numTypes; type++) {
				int[] sourceCounts = sourceTypeTopicCounts[type];
				int[] targetCounts = typeTopicCounts[type];
				
				int sourceIndex = 0;
				while ((sourceIndex < sourceCounts.length) && 
						(sourceCounts[sourceIndex] > 0)) {
					int topic = sourceCounts[sourceIndex] & topicMask;
					int count = sourceCounts[sourceIndex] >> topicBits;
			
					int targetIndex = 0;
					int currentTopic = targetCounts[targetIndex] & topicMask;
					while ((targetCounts[targetIndex] > 0) &&
							(currentTopic != topic)) {
						targetIndex++;
						if (targetIndex == targetCounts.length) {
							logger.info("overflow in merging on type " + type);
						}
						currentTopic = targetCounts[targetIndex] & topicMask;
					}
					
					int currentCount = targetCounts[targetIndex] >> topicBits;
					targetCounts[targetIndex] = ((currentCount + count)
							<< topicBits) + topic;
					
					// Now ensure that the array is still sorted by bubbling
					// this value up.
					while ((targetIndex > 0) && (targetCounts[targetIndex] > 
							targetCounts[targetIndex - 1])) {
						int temp = targetCounts[targetIndex];
						targetCounts[targetIndex] = targetCounts[targetIndex - 1];
						targetCounts[targetIndex - 1] = temp;
						
						targetIndex--;
					}
					
					sourceIndex++;
				}
			}
		}
	}
	
	public void setNumThreads (int numThreads) {
		this.numThreads = numThreads;
	}
	
	public void printState(File f) throws IOException {
		PrintStream out = new PrintStream(
				new BufferedOutputStream( new FileOutputStream(f)));
		out.println("#doc source pos typeindex type topic");
		out.print("#alpha : ");
		for (int topic = 0; topic < numTopics; topic++) {
			out.print(alpha[topic] + " ");
		}
		out.println();
		out.println("#beta : " + beta);
		
		for (int doc = 0; doc < data.size(); doc++) {
			FeatureSequence tokenSequence = (FeatureSequence) data.get(doc).instance.getData();
			LabelSequence topicSequence = (LabelSequence) data.get(doc).topicSequence;
			String source = "NA";
			
			if (data.get(doc).instance.getSource() != null) {
				source = data.get(doc).instance.getSource().toString();
			}
			
			Formatter output = new Formatter(new StringBuilder(), Locale.US);
			
			for (int pi = 0; pi < topicSequence.getLength(); pi++) {
				int type = tokenSequence.getIndexAtPosition(pi);
				int topic = topicSequence.getIndexAtPosition(pi);
				output.format("%d %s %d %d %s %d\n", doc, source, pi, type,
						alphabet.lookupObject(type), topic);
			}
			
			out.print(output);
		}
		out.close();
	}
	// Prints the tweet number, it's status id, and the most common word of the
	// tweet's most common topic.
	public void printTopWord (File f) throws IOException {
		PrintStream out = new PrintStream(
				new BufferedOutputStream( new FileOutputStream(f)));
		out.print("#doc name topic\n");
		ArrayList<TreeSet<IDSorter>> topicSortedWords = getSortedWords();
		IDSorter[] sortedTopics = new IDSorter[numTopics];
		// Initialize the sorter with dummy values.
		for (int topic = 0; topic < numTopics; topic++) {
			sortedTopics[topic] = new IDSorter(topic, topic);
		}
		
		for  (int doc = 0; doc < data.size(); doc++) {
			LabelSequence topicSequence = (LabelSequence) data.get(doc).topicSequence;
			int[] currentDocTopics = topicSequence.getFeatures();
			int docLen;
			int[] topicCounts = new int[numTopics];
			
			out.print(doc);
			out.print("\t");
			
			// Idiot proofing the doc naming
			if (data.get(doc).instance.getName() != null) {
				out.print(data.get(doc).instance.getName());
			}
			else {
				out.print("no-name");
			}
			
			out.print("\t");
			docLen = currentDocTopics.length;
			
			// Count up the tokens
			for (int token = 0; token < docLen; token++) {
				topicCounts[currentDocTopics[token]]++;
			}
			// Normalize and sort the topics
			for (int topic = 0; topic < numTopics; topic++) {
				sortedTopics[topic].set(topic, (alpha[doc][topic] + topicCounts[topic])
						/ (docLen + alphaSum));
			}
			Arrays.sort(sortedTopics);
			TreeSet<IDSorter> sortedWords = topicSortedWords.get(sortedTopics[0].getID());
			Iterator<IDSorter> iterator = sortedWords.iterator();
			IDSorter info = iterator.next();
			out.print(alphabet.lookupObject(info.getID()));
			out.print("\n");
		}
		out.close();
	}
	
	// Returns array of topics, sorted in the same order as their corresponding
	// documents, as in printTopWord.
	public ArrayList<String> getTopicArray(int numWords) {
		ArrayList<String> topicArray = new ArrayList<String>();
		
		ArrayList<TreeSet<IDSorter>> topicSortedWords = getSortedWords();
		IDSorter[] sortedTopics = new IDSorter[numTopics];
		// Initialize the sorter with dummy values.
		for (int topic = 0; topic < numTopics; topic++) {
			sortedTopics[topic] = new IDSorter(topic, topic);
		}
		
		for  (int doc = 0; doc < data.size(); doc++) {
			LabelSequence topicSequence = (LabelSequence) data.get(doc).topicSequence;
			int[] currentDocTopics = topicSequence.getFeatures();
			int docLen;
			int[] topicCounts = new int[numTopics];
			
			docLen = currentDocTopics.length;
			
			// Count up the tokens
			for (int token = 0; token < docLen; token++) {
				topicCounts[currentDocTopics[token]]++;
			}
			// Normalize and sort the topics
			for (int topic = 0; topic < numTopics; topic++) {
				sortedTopics[topic].set(topic, (alpha[doc][topic] + topicCounts[topic])
						/ (docLen + alphaSum));
			}
			Arrays.sort(sortedTopics);
			TreeSet<IDSorter> sortedWords = topicSortedWords.get(sortedTopics[0].getID());
			// This feels like a mess
			Iterator<IDSorter> iterator = sortedWords.iterator();
			StringBuilder out  = new StringBuilder();
			int word = 0;
			while (iterator.hasNext() && word < numWords) {
				IDSorter info = iterator.next();
				out.append(alphabet.lookupObject(info.getID()) + " ");
				word++;
			}
			
			topicArray.add(out.toString());
		}
		return topicArray;
	}
	
	// Print out each topic, and the top documents for that topic.
	public void printTopicDocuments (File file, int topicSize, String[] tweets) throws IOException {
		PrintWriter out = new PrintWriter (new FileWriter (file));
		//int docLen;
		//int[] topicCounts = new int[numTopics];
		ArrayList<TreeSet<IDSorter>> topicSortedWords = getSortedWords();
		IDSorter[] sortedTopics = new IDSorter[numTopics];
		
		
		
		for (int topic = 0; topic < numTopics; topic++) {
			// Initialize the sorters with dummy values.
			sortedTopics[topic] = new IDSorter(topic, topic);
			for (int doc = 0; doc < data.size(); doc++) {
				topicDocCountsHere[topic][doc] += alpha[doc][topic];
			}
		}
		
		for (int topic = 0; topic < numTopics; topic++) {
			StringBuilder builder = new StringBuilder();
			builder.append(topic);
			builder.append("\t");
			
			TreeSet<IDSorter> sortedWords = topicSortedWords.get(topic);
			// This feels like a mess
			Iterator<IDSorter> iterator = sortedWords.iterator();
			
			int word = 0;
			while (iterator.hasNext() && word < topicSize) {
				IDSorter info = iterator.next();
				builder.append(alphabet.lookupObject(info.getID()) + " ");
				word++;
			}
			builder.append("\n");
			
			int docsForThisTopic = topicDocCountsHere[topic].length;
			int[] indexes = new int[docsForThisTopic];
			for (int i = 0; i < docsForThisTopic; i++) {
				indexes[i] = i;
			}
			
			
			
			
			// Sort the documents in descending order of relevance.
			int temp = 0;
			for (int j = 0; j < docsForThisTopic; j++) {
				for (int k = 1; k < (docsForThisTopic - j); k++) {
					//DEBUG
					//System.out.println(topicDocCounts[topic][k]);
					if (topicDocCountsHere[topic][k-1] < topicDocCountsHere[topic][k]) {
						temp = indexes[k-1];
						indexes[k-1] = indexes[k];
						indexes[k] = temp;
					}
				}
			}
			//DEBUG
			/*
			for (int ee = 0; ee < indexes.length; ee++) {
				System.out.print(indexes[ee]);
			}
			System.out.println();
			*/
			// Print out top 10 tweets for this topic
			for (int doc = 0; doc < 10; doc++) {
				builder.append("\t");
				builder.append(tweets[indexes[doc]]);
				builder.append("\n");
				//System.out.print(indexes[doc] + " ");
			}
			//System.out.println();
			out.print(builder);
		}
		
		out.close();
	}
	
	// Return the Jensen-Shannon Divergence for each of the topics.
	public double[] topicJSD() {
		double[] JSDArray = new double[numTopics];
		int[][] prevArray = new int[numTopics][prevTypeTopicCounts.length];
		int[][] currentArray = new int[numTopics][prevTypeTopicCounts.length];
		int[] currentSums = new int[numTopics];
		int[] prevSums = new int[numTopics];
		
		
		for (int type = 0; type < prevTypeTopicCounts.length; type++) {
			int[] currentCounts = typeTopicCounts[type];
			int index = 0;
			while ((index < currentCounts.length) && 
					currentCounts[index] > 0) {
				int topic = currentCounts[index] & topicMask;
				int count = currentCounts[index] >> topicBits;
				currentArray[topic][type] = count;
				currentSums[topic] += count;
				index++;
			}
			int[] prevCounts = prevTypeTopicCounts[type];
			index = 0;
			while ((index < prevCounts.length) && 
					prevCounts[index] > 0) {
				int topic = prevCounts[index] & topicMask;
				int count = prevCounts[index] >> topicBits;
				prevArray[topic][type] = count;
				prevSums[topic] += count;
				index++;
			}
		}
			
		for (int topic = 0; topic < numTopics; topic++) {	
			
			// Normalize
			double[] prevNormal = new double[prevTypeTopicCounts.length];
			double[] currentNormal = new double[prevTypeTopicCounts.length];
			//DEBUG
			//System.out.println(prevSum + ", " + currentSum);
			for (int type = 0; type < prevArray.length; type++) {
				prevNormal[type] = (double)prevArray[topic][type] / (double)prevSums[topic];
				currentNormal[type] = (double)currentArray[topic][type] / (double)currentSums[topic];
			}
			
			JSDArray[topic] = Maths.jensenShannonDivergence(prevNormal, currentNormal);
		}
		
		return JSDArray;
	}
	
	// Calculate topic coherence
	/*
	public double calcAssoc(int word1, int word2) {
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
	*/
	
	/*
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
	*/
	
	// Getter methods for alphaSum, beta, numTypes
	public double getAlphaSum() {
		return alphaSum;
	}
	public double[][] getBeta() {
		return beta;
	}
	public int getNumTypes() {
		return numTypes;
	}
	public void setOldNumTypes(int old) {
		oldNumTypes = old;
	}
	public int getNumDocs() {
		return data.size();
	}
	public void setOldNumDocs(int docs) {
		oldNumDocs = docs;
	}
	public int getNumTopics() {
		return numTopics;
	}
	public int getNumTokens() {
		return totalTokens;
	}
	public void setOldTokens(int tokens) {
		oldTokens = tokens;
	}
	public int[][] getAlphaCounts() {
		int[][] array = new int[alphaCounts.length][];
		for (int i = 0; i < alphaCounts.length; i++) {
			array[i] = Arrays.copyOf(alphaCounts[i], alphaCounts[i].length);
			//for (int j = 0; j < alphaCounts[i].length; j++) {
			//	array[i][j] = alphaCounts[i][j];
			//}
		}
		return array;
	}
	
	public void setPrevAlphaCounts(int[][] counts) {
		prevAlphaCounts = new int[counts.length][];
		for (int i = 0; i < counts.length; i++) {
			prevAlphaCounts[i] = Arrays.copyOf(counts[i], counts[i].length);
			//for (int j = 0; j < counts[i].length; j++) {
			//	this.prevAlphaCounts[i][j] = counts[i][j];
			//}
		}
	}
	public int[][] getTypeTopicCounts() {
		int[][] array = new int[typeTopicCounts.length][];
		for (int i = 0; i < typeTopicCounts.length; i++) {
			array[i] = Arrays.copyOf(typeTopicCounts[i], typeTopicCounts[i].length);
		}
		return array;
	}
	public void setPrevTypeTopicCounts(int[][] counts) {
		prevTypeTopicCounts = new int[counts.length][];
		for (int i = 0; i < counts.length; i++) {
			prevTypeTopicCounts[i] = Arrays.copyOf(counts[i], counts[i].length);
			//for (int j = 0; j < counts[i].length; j++) {
			//	this.prevTypeTopicCounts[i][j] = counts[i][j];
			//}
		}
	}
	public Alphabet getAlphabet() {
		return alphabet;
	}
	public void setAlphabet(Alphabet al) {
		alphabet = new Alphabet();
		alphabet = al;
	}
	
	//TODO
	// Drop the previous instances from the list, keeping everything else in position.
	// Same for the alphabet/vocabulary.
	public void dropInstances(int numDropped) {
		for (TopicAssignment document : data) {
			FeatureSequence tokens = (FeatureSequence)document.instance.getData();
			LabelSequence topicSequence = (LabelSequence)document.topicSequence;
			int[] topics = topicSequence.getFeatures();
			
			for (int position = 0; position < tokens.size(); position++) {
				int topic = topics[position];
				int type = tokens.getIndexAtPosition(position);
				typeTopicCounts[type][topic]--;
				
			}
		}
		
		data.subList(0, data.size() - numDropped).clear();
		
	}
}
