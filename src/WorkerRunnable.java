/* Copyright (C) 2005 Univ. of Massachusetts Amherst, Computer Science Dept.
   This file is part of "MALLET" (MAchine Learning for LanguagE Toolkit).
   http://www.cs.umass.edu/~mccallum/mallet
   This software is provided under the terms of the Common Public License,
   version 1.0, as published by http://www.opensource.org.	For further
   information, see the file `LICENSE' included with this distribution. */

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.commons.lang.ArrayUtils;

//import java.util.zip.*;

//import java.io.*;
//import java.text.NumberFormat;

import cc.mallet.types.*;
import cc.mallet.topics.TopicAssignment;
import cc.mallet.util.Randoms;

/**
 * A parallel topic model runnable task.
 * 
 * @author David Mimno, Andrew McCallum
 * Modified on 8/2/2016 by Ezekiel Robertson, fixing a couple of errors and
 * adding a getter method for isFinished. 
 */

public class WorkerRunnable implements Runnable {
	int UNASSIGNED_TOPIC = -1;
	boolean isFinished = true;

	ArrayList<TopicAssignment> data;
	int startDoc, numDocs;
	int[][] docStorageArray;

	protected int numTopics; // Number of topics to be fit

	// These values are used to encode type/topic counts as
	//  count/topic pairs in a single int.
	protected int topicMask;
	protected int topicBits;

	protected int numTypes;

	protected double[][] alpha;	 // Dirichlet(alpha,alpha,...) is the distribution over topics
	protected double alphaSum;
	protected double[][] beta;   // Prior on per-topic multinomial distribution over words
	protected double betaSum;
	protected double[] betaAvg;
	protected double[] alphaAvg;
	public static final double DEFAULT_BETA = 0.01;
	
	protected double smoothingOnlyMass = 0.0;
	protected double[] cachedCoefficients;

	protected int[][] typeTopicCounts; // indexed by <feature index, topic index>
	protected int[] tokensPerTopic; // indexed by <topic index>

	// for dirichlet estimation
	protected int[] docLengthCounts; // histogram of document sizes
	public int[][] topicDocCounts; // histogram of document/topic counts, indexed by <topic index, sequence position index>

	boolean shouldSaveState = true;
	boolean shouldBuildLocalCounts = true;
	
	protected Randoms random;
	
	public WorkerRunnable (int numTopics,
						   double[][] alpha, double alphaSum,
						   double[][] beta, double betaSum, int newTypes, int oldDocs, 
						   Randoms random,
						   ArrayList<TopicAssignment> data,
						   int[][] typeTopicCounts, 
						   int[] tokensPerTopic,
						   int startDoc, int numDocs) {

		this.data = data;

		this.numTopics = numTopics;
		this.numTypes = typeTopicCounts.length;

		if (Integer.bitCount(numTopics) == 1) {
			// exact power of 2
			topicMask = numTopics - 1;
			topicBits = Integer.bitCount(topicMask);
		}
		else {
			// otherwise add an extra bit
			topicMask = Integer.highestOneBit(numTopics) * 2 - 1;
			topicBits = Integer.bitCount(topicMask);
		}

		this.typeTopicCounts = typeTopicCounts;
		this.tokensPerTopic = tokensPerTopic;
		
		this.alphaSum = alphaSum;
		this.alpha = alpha;
		this.betaSum = betaSum;
		this.beta = beta;
		//DEBUG
		//System.out.println("Is beta 0? " + beta[0][0]);
		//this.betaSum = 0;
		this.betaAvg = new double[numTopics];
		int topic = 0;
		for (double[] i : beta) {
			for (double j : i) {
				this.betaAvg[topic] += j;
		//		this.betaSum += j; 
			}
			this.betaAvg[topic] /= i.length;
			topic++;
		}
		
		this.alphaAvg = new double[numTopics];
		for (topic = 0; topic < numTopics; topic++) {
			for (int doc = startDoc; doc < data.size() && doc < startDoc + numDocs; doc++) {
				alphaAvg[topic] += alpha[doc][topic];
			}
			this.betaAvg[topic] /= numDocs;
		}
		
		
		this.random = random;
		
		this.startDoc = startDoc;
		this.numDocs = numDocs;

		cachedCoefficients = new double[ numTopics ];
		this.topicDocCounts = new int[numTopics][numDocs];
		this.docStorageArray = new int[numTopics][numDocs];

		//System.err.println("WorkerRunnable Thread: " + numTopics + " topics, " + topicBits + " topic bits, " + 
		//				   Integer.toBinaryString(topicMask) + " topic mask");

	}

	/**
	 *  If there is only one thread, we don't need to go through 
	 *   communication overhead. This method asks this worker not
	 *   to prepare local type-topic counts. The method should be
	 *   called when we are using this code in a non-threaded environment.
	 */
	public void makeOnlyThread() {
		shouldBuildLocalCounts = false;
	}

	public int[] getTokensPerTopic() { return tokensPerTopic; }
	public int[][] getTypeTopicCounts() { return typeTopicCounts; }

	public int[] getDocLengthCounts() { return docLengthCounts; }
	public int[][] getTopicDocCounts() {
		int[][] array = new int[topicDocCounts.length][];
		//DEBUG
		//System.out.println(topicDocCounts.length + ", " + topicDocCounts[0].length);
		for (int i = 0; i < topicDocCounts.length; i++) {
			array[i] = Arrays.copyOf(topicDocCounts[i], topicDocCounts[i].length);
			//System.out.println(array[i][0] + ", " + topicDocCounts[i][0] + ", " 
			//		+ docStorageArray[i][0]);
		}
		return array;
	}
	
	public boolean getIsFinished() { return isFinished; }

	public void initializeAlphaStatistics(int size) {
		docLengthCounts = new int[size];
		
	}
	
	public void collectAlphaStatistics() {
		shouldSaveState = true;
	}

	public void resetBeta(double[][] beta, double betaSum) {
		this.beta = beta;
		this.betaSum = betaSum;
	}

	/**
	 *  Once we have sampled the local counts, trash the 
	 *   "global" type topic counts and reuse the space to 
	 *   build a summary of the type topic counts specific to 
	 *   this worker's section of the corpus.
	 */
	public void buildLocalTypeTopicCounts () {

		// Clear the topic totals
		Arrays.fill(tokensPerTopic, 0);

		// Clear the type/topic counts, only 
		//  looking at the entries before the first 0 entry.

		for (int type = 0; type < typeTopicCounts.length; type++) {

			int[] topicCounts = typeTopicCounts[type];
			
			int position = 0;
			while (position < topicCounts.length && 
				   topicCounts[position] > 0) {
				topicCounts[position] = 0;
				position++;
			}
		}

        for (int doc = startDoc;
			 doc < data.size() && doc < startDoc + numDocs;
             doc++) {

			TopicAssignment document = data.get(doc);

            FeatureSequence tokens = (FeatureSequence) document.instance.getData();
            LabelSequence topicSequence =  (LabelSequence) document.topicSequence;

            int[] topics = topicSequence.getFeatures();
            for (int position = 0; position < tokens.size(); position++) {

				int topic = topics[position];

				if (topic == UNASSIGNED_TOPIC) { continue; }

				tokensPerTopic[topic]++;
				
				// The format for these arrays is 
				//  the topic in the rightmost bits
				//  the count in the remaining (left) bits.
				// Since the count is in the high bits, sorting (desc)
				//  by the numeric value of the int guarantees that
				//  higher counts will be before the lower counts.
				
				int type = tokens.getIndexAtPosition(position);

				int[] currentTypeTopicCounts = typeTopicCounts[ type ];
				
				// Start by assuming that the array is either empty
				//  or is in sorted (descending) order.
				
				// Here we are only adding counts, so if we find 
				//  an existing location with the topic, we only need
				//  to ensure that it is not larger than its left neighbor.
				
				int index = 0;
				int currentTopic = currentTypeTopicCounts[index] & topicMask;
				int currentValue;
				
				while (currentTypeTopicCounts[index] > 0 && currentTopic != topic) {
					index++;
					if (index == currentTypeTopicCounts.length) {
						System.out.println("overflow on type " + type);
					}
					currentTopic = currentTypeTopicCounts[index] & topicMask;
				}
				currentValue = currentTypeTopicCounts[index] >> topicBits;
				
				if (currentValue == 0) {
					// new value is 1, so we don't have to worry about sorting
					//  (except by topic suffix, which doesn't matter)
					
					currentTypeTopicCounts[index] =
						(1 << topicBits) + topic;
				}
				else {
					currentTypeTopicCounts[index] =
						((currentValue + 1) << topicBits) + topic;
					
					// Now ensure that the array is still sorted by 
					//  bubbling this value up.
					while (index > 0 &&
						   currentTypeTopicCounts[index] > currentTypeTopicCounts[index - 1]) {
						int temp = currentTypeTopicCounts[index];
						currentTypeTopicCounts[index] = currentTypeTopicCounts[index - 1];
						currentTypeTopicCounts[index - 1] = temp;
						
						index--;
					}
				}
			}
		}

	}


	public void run () {

		try {
			
			if (! isFinished) { System.out.println("already running!"); return; }
			
			isFinished = false;
			
			// Initialize the smoothing-only sampling bucket
			smoothingOnlyMass = 0;
			
			// Initialize the cached coefficients, using only smoothing.
			//  These values will be selectively replaced in documents with
			//  non-zero counts in particular topics.
			//TODO find type index for beta, alpha
			for (int topic=0; topic < numTopics; topic++) {
				smoothingOnlyMass += alphaAvg[topic] * betaAvg[topic] / (tokensPerTopic[topic] + betaSum);
				cachedCoefficients[topic] =  alphaAvg[topic] / (tokensPerTopic[topic] + betaSum);
			}
			
			for (int doc = startDoc;
				 doc < data.size() && doc < startDoc + numDocs;
				 doc++) {
				
				/*
				  if (doc % 10000 == 0) {
				  System.out.println("processing doc " + doc);
				  }
				*/
				
				FeatureSequence tokenSequence =
					(FeatureSequence) data.get(doc).instance.getData();
				LabelSequence topicSequence =
					(LabelSequence) data.get(doc).topicSequence;
				
				sampleTopicsForOneDoc (tokenSequence, topicSequence, doc,
									   true);
			}
			
			if (shouldBuildLocalCounts) {
				buildLocalTypeTopicCounts();
			}

			shouldSaveState = false;
			isFinished = true;

		} catch (Exception e) {
			isFinished = true;
			e.printStackTrace();
		}
	}
	
	public void sampleTopicsForOneDoc (FeatureSequence tokenSequence,
										  LabelSequence topicSequence,
										  int currentDoc,
										  boolean readjustTopicsAndStats /* currently ignored */) {

		int[] oneDocTopics = topicSequence.getFeatures();
		int[] currentTypeTopicCounts;
		int type, oldTopic, newTopic;
//		double topicWeightsSum;
		int docLength = tokenSequence.getLength();

		int[] localTopicCounts = new int[numTopics];
		int[] localTopicIndex = new int[numTopics];

		//		populate topic counts
		for (int position = 0; position < docLength; position++) {
			if (oneDocTopics[position] == UNASSIGNED_TOPIC) { continue; }
			localTopicCounts[oneDocTopics[position]]++;
		}

		// Build an array that densely lists the topics that
		//  have non-zero counts.
		int denseIndex = 0;
		for (int topic = 0; topic < numTopics; topic++) {
			if (localTopicCounts[topic] != 0) {
				localTopicIndex[denseIndex] = topic;
				denseIndex++;
			}
		}

		// Record the total number of non-zero topics
		int nonZeroTopics = denseIndex;

		//		Initialize the topic count/beta sampling bucket
		double topicBetaMass = 0.0;

		// Initialize cached coefficients and the topic/beta 
		//  normalizing constant.

		for (denseIndex = 0; denseIndex < nonZeroTopics; denseIndex++) {
			int topic = localTopicIndex[denseIndex];
			int n = localTopicCounts[topic];

			//	initialize the normalization constant for the (B * n_{t|d}) term
			//TODO find type index for beta
			topicBetaMass += betaAvg[topic] * n / (tokensPerTopic[topic] + betaSum);
			
			//	update the coefficients for the non-zero topics
			cachedCoefficients[topic] =	(alpha[currentDoc][topic] + n) / (tokensPerTopic[topic] + betaSum);
			//DEBUG
			//System.out.println(topicBetaMass);
		}
		
		double topicTermMass = 0.0;

		double[] topicTermScores = new double[numTopics];
		//int[] topicTermIndices;
		//int[] topicTermValues;
		int i;
		double score;

		//	Iterate over the positions (words) in the document 
		for (int position = 0; position < docLength; position++) {
			type = tokenSequence.getIndexAtPosition(position);
			oldTopic = oneDocTopics[position];

			currentTypeTopicCounts = typeTopicCounts[type];

			if (oldTopic != UNASSIGNED_TOPIC) {
				//	Remove this token from all counts. 
				
				// Remove this topic's contribution to the 
				//  normalizing constants
				smoothingOnlyMass -= alpha[currentDoc][oldTopic] * beta[oldTopic][type] / 
					(tokensPerTopic[oldTopic] + betaSum);
				topicBetaMass -= beta[oldTopic][type] * localTopicCounts[oldTopic] /
					(tokensPerTopic[oldTopic] + betaSum);
				
				// Decrement the local doc/topic counts
				
				localTopicCounts[oldTopic]--;
				
				// Maintain the dense index, if we are deleting
				//  the old topic
				if (localTopicCounts[oldTopic] == 0) {
					
					// First get to the dense location associated with
					//  the old topic.
					
					denseIndex = 0;
					
					// We know it's in there somewhere, so we don't 
					//  need bounds checking.
					while (localTopicIndex[denseIndex] != oldTopic) {
						denseIndex++;
					}
				
					// shift all remaining dense indices to the left.
					while (denseIndex < nonZeroTopics) {
						if (denseIndex < localTopicIndex.length - 1) {
							localTopicIndex[denseIndex] = 
								localTopicIndex[denseIndex + 1];
						}
						denseIndex++;
					}
					
					nonZeroTopics --;
				}

				// Decrement the global topic count totals
				tokensPerTopic[oldTopic]--;
				assert(tokensPerTopic[oldTopic] >= 0) : "old Topic " + oldTopic + " below 0";
				
				//DEBUG
				/*
				if (topicBetaMass < 0 && localTopicCounts[oldTopic] == 0) {
					System.out.println("KNOCK, KNOCK");
				}
				*/

				// Add the old topic's contribution back into the
				//  normalizing constants.
				smoothingOnlyMass += alpha[currentDoc][oldTopic] * beta[oldTopic][type] / 
					(tokensPerTopic[oldTopic] + betaSum);
				topicBetaMass += beta[oldTopic][type] * localTopicCounts[oldTopic] /
					(tokensPerTopic[oldTopic] + betaSum);

				// Reset the cached coefficient for this topic
				cachedCoefficients[oldTopic] = 
					(alpha[currentDoc][oldTopic] + localTopicCounts[oldTopic]) /
					(tokensPerTopic[oldTopic] + betaSum);
			}


			// Now go over the type/topic counts, decrementing
			//  where appropriate, and calculating the score
			//  for each topic at the same time.

			int index = 0;
			int currentTopic, currentValue;

			boolean alreadyDecremented = (oldTopic == UNASSIGNED_TOPIC);

			topicTermMass = 0.0;

			while (index < currentTypeTopicCounts.length && 
					   currentTypeTopicCounts[index] > 0) {
				currentTopic = currentTypeTopicCounts[index] & topicMask;
				currentValue = currentTypeTopicCounts[index] >> topicBits;
				
				// TODO: bad stuff here to get around error.
				
				if (currentTopic >= numTopics) {
					currentTopic = random.nextInt(numTopics);
				}
				
				
				if (! alreadyDecremented && 
					currentTopic == oldTopic) {

					// We're decrementing and adding up the 
					//  sampling weights at the same time, but
					//  decrementing may require us to reorder
					//  the topics, so after we're done here,
					//  look at this cell in the array again.

					currentValue --;
					if (currentValue == 0) {
						currentTypeTopicCounts[index] = 0;
					}
					else {
						currentTypeTopicCounts[index] =
							(currentValue << topicBits) + oldTopic;
					}
					
					// Shift the reduced value to the right, if necessary.

					int subIndex = index;
					while (subIndex < currentTypeTopicCounts.length - 1 && 
						   currentTypeTopicCounts[subIndex] < currentTypeTopicCounts[subIndex + 1]) {
						int temp = currentTypeTopicCounts[subIndex];
						currentTypeTopicCounts[subIndex] = currentTypeTopicCounts[subIndex + 1];
						currentTypeTopicCounts[subIndex + 1] = temp;
							
						subIndex++;
					}

					alreadyDecremented = true;
				}
				else {
					//DEBUG
					//System.out.println(cachedCoefficients.length+" : "+currentTopic);
					score = 
						cachedCoefficients[currentTopic] * currentValue;
					topicTermMass += score;
					topicTermScores[index] = score;

					index++;
				}
			}
			
			double sample = random.nextUniform() * Math.abs(smoothingOnlyMass + topicBetaMass + topicTermMass);
			// DEBUG
			//System.out.println(sample+", "+smoothingOnlyMass+", "+topicBetaMass+", "+topicTermMass);
			
			if (sample < 0) {
				System.err.println("Sample: " + sample);
				System.err.println("SOM: " + smoothingOnlyMass + ", TBM: " + 
						topicBetaMass + ", TTM: " + topicTermMass);
				System.exit(0);
			}
			
			double origSample = sample;

			//	Make sure it actually gets set
			newTopic = -1;
			// FIND THE F****** ERROR
			int EXTERMINATOR = 0;

			if (sample < topicTermMass) {
				EXTERMINATOR = 1;
				//topicTermCount++;

				i = -1;
				while (sample > 0) {
					i++;
					sample -= topicTermScores[i];
				}
				
				newTopic = currentTypeTopicCounts[i] & topicMask;
				currentValue = currentTypeTopicCounts[i] >> topicBits;
				
				currentTypeTopicCounts[i] = ((currentValue + 1) << topicBits) + newTopic;

				// Bubble the new value up, if necessary
				
				while (i > 0 &&
					   currentTypeTopicCounts[i] > currentTypeTopicCounts[i - 1]) {
					int temp = currentTypeTopicCounts[i];
					currentTypeTopicCounts[i] = currentTypeTopicCounts[i - 1];
					currentTypeTopicCounts[i - 1] = temp;

					i--;
				}

			}
			else {
				sample -= topicTermMass;
				//DEBUG
				/*
				if (sample > origSample) {
					System.out.println("Sample > origSample, s > TTM");
				}
				*/
				// This is a problem section
				if (sample < topicBetaMass) {
					EXTERMINATOR = 2;
					//betaTopicCount++;
					sample /= beta[oldTopic][type];

					for (denseIndex = 0; denseIndex < nonZeroTopics; denseIndex++) {
						
						int topic = localTopicIndex[denseIndex];
						// New addition, hopelfully this fixes stuff;
						newTopic = topic;
						
						sample -= localTopicCounts[topic] /
							(tokensPerTopic[topic] + betaSum);

						if (sample <= 0.0) {
							newTopic = topic;
							break;
						}
					}

				}
				// End of problem section
				else {
					EXTERMINATOR = 3;
					//DEBUG
					/*
					if (sample > origSample) {
						System.out.println("Sample > origSample, s > TBM");
					}
					*/
					//smoothingOnlyCount++;

					sample -= topicBetaMass;
					
					sample /= beta[oldTopic][type];

					newTopic = 0;
					sample -= alpha[currentDoc][newTopic] /
						(tokensPerTopic[newTopic] + betaSum);
					
					
					while (sample > 0.0 && newTopic < numTopics - 1) {
						// DEBUG
						//System.err.println(sample);
						newTopic++;
						sample -= alpha[currentDoc][newTopic] / 
							(tokensPerTopic[newTopic] + betaSum);
					}
					
				}

				// Move to the position for the new topic,
				//  which may be the first empty position if this
				//  is a new topic for this word.
				
				index = 0;
				while (currentTypeTopicCounts[index] > 0 &&
					   (currentTypeTopicCounts[index] & topicMask) != newTopic) {
					index++;
					if (index == currentTypeTopicCounts.length) {
						System.err.println("type: " + type + " new topic: " + newTopic);
						for (int k=0; k<currentTypeTopicCounts.length; k++) {
							System.err.print((currentTypeTopicCounts[k] & topicMask) + ":" + 
											 (currentTypeTopicCounts[k] >> topicBits) + " ");
						}
						System.err.println();

					}
				}


				// index should now be set to the position of the new topic,
				//  which may be an empty cell at the end of the list.

				if (currentTypeTopicCounts[index] == 0) {
					// inserting a new topic, guaranteed to be in
					//  order w.r.t. count, if not topic.
					currentTypeTopicCounts[index] = (1 << topicBits) + newTopic;
				}
				else {
					currentValue = currentTypeTopicCounts[index] >> topicBits;
					currentTypeTopicCounts[index] = ((currentValue + 1) << topicBits) + newTopic;

					// Bubble the increased value left, if necessary
					while (index > 0 &&
						   currentTypeTopicCounts[index] > currentTypeTopicCounts[index - 1]) {
						int temp = currentTypeTopicCounts[index];
						currentTypeTopicCounts[index] = currentTypeTopicCounts[index - 1];
						currentTypeTopicCounts[index - 1] = temp;

						index--;
					}
				}

			}

			if (newTopic == -1) {
				System.err.println("WorkerRunnable sampling error: "+ origSample
						+ " " + sample + " " + smoothingOnlyMass + " " + 
						topicBetaMass + " " + topicTermMass + " " + EXTERMINATOR);
				newTopic = numTopics-1; // TODO is this appropriate
				// No it is not. Everything is terrible.
				//throw new IllegalStateException ("WorkerRunnable: New topic not sampled.");
			}
			//assert(newTopic != -1);

			//			Put that new topic into the counts
			oneDocTopics[position] = newTopic;

			smoothingOnlyMass -= alpha[currentDoc][newTopic] * beta[newTopic][type] / 
				(tokensPerTopic[newTopic] + betaSum);
			topicBetaMass -= beta[newTopic][type] * localTopicCounts[newTopic] /
				(tokensPerTopic[newTopic] + betaSum);

			localTopicCounts[newTopic]++;

			// If this is a new topic for this document,
			//  add the topic to the dense index.
			if (localTopicCounts[newTopic] == 1) {
				
				// First find the point where we 
				//  should insert the new topic by going to
				//  the end (which is the only reason we're keeping
				//  track of the number of non-zero
				//  topics) and working backwards

				denseIndex = nonZeroTopics;

				while (denseIndex > 0 &&
					   localTopicIndex[denseIndex - 1] > newTopic) {

					localTopicIndex[denseIndex] =
						localTopicIndex[denseIndex - 1];
					denseIndex--;
				}
				
				localTopicIndex[denseIndex] = newTopic;
				nonZeroTopics++;
			}

			tokensPerTopic[newTopic]++;

			//	update the coefficients for the non-zero topics
			cachedCoefficients[newTopic] =
				(alpha[currentDoc][newTopic] + localTopicCounts[newTopic]) /
				(tokensPerTopic[newTopic] + betaSum);

			smoothingOnlyMass += alpha[currentDoc][newTopic] * beta[newTopic][type] / 
				(tokensPerTopic[newTopic] + betaSum);
			topicBetaMass += beta[newTopic][type] * localTopicCounts[newTopic] /
				(tokensPerTopic[newTopic] + betaSum);

		}

		if (shouldSaveState) {
			// Update the document-topic count histogram,
			//  for dirichlet estimation
			docLengthCounts[ docLength ]++;

			for (denseIndex = 0; denseIndex < nonZeroTopics; denseIndex++) {
				int topic = localTopicIndex[denseIndex];
				
				topicDocCounts[topic][currentDoc - startDoc]++;
				//docStorageArray[topic][currentDoc - startDoc] = 
				//		topicDocCounts[topic][currentDoc - startDoc];
				//DEBUG
				//System.out.println(topicDocCounts[topic][ localTopicCounts[topic] ]);
			}
		}

		//	Clean up our mess: reset the coefficients to values with only
		//	smoothing. The next doc will update its own non-zero topics...

		for (denseIndex = 0; denseIndex < nonZeroTopics; denseIndex++) {
			int topic = localTopicIndex[denseIndex];

			cachedCoefficients[topic] =
				alpha[currentDoc][topic] / (tokensPerTopic[topic] + betaSum);
		}

	}

}
