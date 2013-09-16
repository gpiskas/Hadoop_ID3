/*
* Hadoop ID3
* Copyright (C) 2013 George Piskas
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation; either version 2 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License along
* with this program; if not, write to the Free Software Foundation, Inc.,
* 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*
* Contact: geopiskas@gmail.com
*/

package id3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.StringTokenizer;

import main.Utils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import wine.QualityStats;
import wine.Wine;
import wine.WineComparator;

public class WineMapper extends Mapper<LongWritable, Text, Text, Text> {

	// Checks if the path contains all the attributes.
	private boolean allAttributesChecked(String path) {
		return (path.contains(Wine.S_FI) 
				&& path.contains(Wine.S_VO)
				&& path.contains(Wine.S_CI) 
				&& path.contains(Wine.S_RE)
				&& path.contains(Wine.S_CH) 
				&& path.contains(Wine.S_FR)
				&& path.contains(Wine.S_TO) 
				&& path.contains(Wine.S_DE)
				&& path.contains(Wine.S_PH) 
				&& path.contains(Wine.S_SU)
				&& path.contains(Wine.S_AL) 
				&& path.contains(Wine.S_QU));
	}

	/* Given a list of cut point indexes, the cut attribute and the wine list sorted by that attribute,
	 * extends the current path with all possible combinations of ranges.
	 */
	private ArrayList<String> getNewPaths(String path, ArrayList<Wine> wines, ArrayList<Integer> cuts, int attr) {
		ArrayList<String> paths = new ArrayList<String>();
		String name = Wine.nameOf(attr);
		
		// Creates the extended paths that consist of the in-between ranges.
		for (int i = 0; (i < cuts.size() - 1) && (cuts.size()) > 1; i++) {
			paths.add(path + "," + name + " " + wines.get(cuts.get(i)).valueOf(attr) + " "	+ wines.get(cuts.get(i + 1)).valueOf(attr));
		}
		
		// Creates the two additional paths that consist of the boundary ranges.
		paths.add(path + "," + name + " " + wines.get(0).valueOf(attr) + " " + wines.get(cuts.get(0)).valueOf(attr));
		paths.add(path + "," + name + " " + wines.get(cuts.get(cuts.size() - 1)).valueOf(attr) + " " + wines.get(wines.size() - 1).valueOf(attr));

		return paths;
	}

	// Creates an array of quality stats for the wines of each range. Then calculates the total information gain.
	private float getCutInfoGain(ArrayList<Integer> cuts, ArrayList<Wine> wines, float initEnt) {
		
		// If there are no cuts, the information gain is zero.
		if (cuts.size() == 0) {
			return 0;
		}
		QualityStats[] qu = new QualityStats[cuts.size() + 1];
		int i;
		
		// Creates the quality stats that consist of the wines sublist. The sublist range consists of in-between cut indexes. 
		for (i = 0; i < cuts.size() - 1; i++) {
			qu[i] = new QualityStats(new ArrayList<Wine>(wines.subList( cuts.get(i) + 1, cuts.get(i + 1))));
		}
		
		// Creates the two additional quality stats that consist of the boundary wines sublists.
		qu[i++] = new QualityStats(new ArrayList<Wine>(wines.subList(0, cuts.get(0))));
		qu[i] = new QualityStats(new ArrayList<Wine>(wines.subList(cuts.get(cuts.size() - 1) + 1, wines.size())));

		return Utils.infoGain(qu, initEnt);
	}

	/*
	 * Recursive discretization of wine list "wines" based on the attribute index "attr".
	 * Discretization outputs a list of cut values. 
	 * 
	 * In each step, the best split point is calculated and two sublists, S1 and S2, are formed
	 * by splitting the initial list S. If the split provides a sufficient amount of information 
	 * gain (exceeding a criterion), then adds the cut to the "cuts" list and then recursively 
	 * calls cut function for each sublist. 
	 */
	private void cut(ArrayList<Wine> wines, ArrayList<Float> cuts, int attr) {
		
		// If there are no wines available, order more.
		if (wines.size() == 0) {
			return;
		}

		// Creates quality stats instance for each future sublist.
		QualityStats tmpS1Qu = new QualityStats();
		QualityStats tmpS2Qu = new QualityStats(wines);
		
		// Calculates the initial entropy of the uncut "wines" list. At this point S2 equals S.
		float entropyOfS = tmpS2Qu.getEntropy();
		int k = tmpS2Qu.getActiveClassCount();	
		
		float maxInfoGain = -1;
		float maxCutValue = -1;
		int maxCutIndex = -1;
		
		/* In each iteration, gets the first wine of S2 and places it in S1. Then,
		 * calculates the info gain of the new split. If it is greater than the current
		 * maximum, store the cut index and cut value.
		 */
		for (int i = 0; i < wines.size(); i++) {

			int quality = wines.get(i).getQu();
			tmpS1Qu.increment(quality);
			tmpS2Qu.decrement(quality);

			// This check ensures that the last index of a series of same values is stored.
			if ((i + 1) < wines.size() && wines.get(i + 1).valueOf(attr) == wines.get(i).valueOf(attr)) {
				continue;
			}

			// Maximum information gain check.
			float tmpGain = Utils.infoGain(new QualityStats[] {tmpS1Qu, tmpS2Qu}, entropyOfS);
			if (tmpGain > maxInfoGain) {
				maxInfoGain = tmpGain;
				maxCutIndex = i;
				maxCutValue = wines.get(i).valueOf(attr);
			}
		}
		
		int totalWines = wines.size();
		
		// Sublist creation based on the best cut index.
		ArrayList<Wine> s1 = new ArrayList<Wine>(wines.subList(0, maxCutIndex));
		ArrayList<Wine> s2 = new ArrayList<Wine>(wines.subList(maxCutIndex + 1, wines.size()));
		
		QualityStats s1Qu = new QualityStats(s1);
		QualityStats s2Qu = new QualityStats(s2);
		
		float entropyOfS1 = s1Qu.getEntropy();
		float entropyOfS2 = s2Qu.getEntropy();

		int k1 = s1Qu.getActiveClassCount();
		int k2 = s2Qu.getActiveClassCount();

		/* Criterion evaluation. If the calculated information gain is greater than the lower bound set
		 * by the criterion, add the best cut value to the "cuts" list and recursively repeat the 
		 * process for S1 and S2.  
		 */
		float gain = Utils.infoGain(new QualityStats[] { s1Qu, s2Qu }, entropyOfS);
		
		float delta = Utils.log2((float) Math.pow(3, k) - 2) - k * entropyOfS + k1 * entropyOfS1 + k2 * entropyOfS2;

		if (gain > (Utils.log2(totalWines - 1) / totalWines) + (delta / totalWines)) {
			
			// If the best cut is calculated to be either zero or wines.size()-1, there is no point cutting. 
			if (maxCutIndex != 0 && maxCutIndex != wines.size() - 1) {
				cuts.add(maxCutValue);
			}
			
			// Recursive call for S1 and S2.
			cut(s1, cuts, attr);
			cut(s2, cuts, attr);
		} else {
			return;
		}
	}

	public void map(LongWritable key, Text content, Context context) throws IOException, InterruptedException {
		
		// Input filename - path so far.
		String path = ((FileSplit) context.getInputSplit()).getPath().getName();
		String records = content.toString();

		// Iterates over each record and creates a list of wines.
		ArrayList<Wine> wines = new ArrayList<Wine>();
		Wine wineAttr = null;
		StringTokenizer lineTok = new StringTokenizer(records);
		StringTokenizer recordTok;
		String[] attributes = new String[12];
		while (lineTok.hasMoreTokens()) {
			recordTok = new StringTokenizer(lineTok.nextToken(), ",");
			for (int i = 0; i < 12; i++) {
				attributes[i] = recordTok.nextToken();
			}
			wineAttr = new Wine(path, attributes);
			wines.add(wineAttr);
		}
		
		// Calculates the quality stats of the previous wine list.
		QualityStats qu = new QualityStats(wines);

		/* If the path is a leaf, count class percentages and append them to the current path.
		 * Then, a key-value pair that consists of key = path, value = empty file is sent to
		 * the reducer.
		 */
		if (allAttributesChecked(path) || qu.getActiveClassCount() == 1) {
			context.write(qu.getQualityPercentages(path), new Text(""));
			return;
		}

		float initEnt = qu.getEntropy();

		/* If the path is not a leaf, discretize all active wine attributes, find the one with
		 * the best information gain and proceed to extended path creation. 
		 */
		int bestCutIndex = -1;
		float maxInfoGain = -1;
		
		/* A map structure that holds the split indexes of each active attribute.
		 * Key is the attribute index.
		 * Value is a list of splits of the above attribute.
		 */
		HashMap<Integer, ArrayList<Integer>> attrCuts = new HashMap<Integer, ArrayList<Integer>>();
		for (Integer attr : wineAttr.getActiveAttributes()) {
			
			// Sort the list based on attribute index "attr".
			Collections.sort(wines, new WineComparator(attr));
			
			// A list that holds the cut values.
			ArrayList<Float> cuts = new ArrayList<Float>();
			
			// Recursively discretize the given attribute.
			cut(wines, cuts, attr);

			// Sort and reverse the previous "cuts" list.
			Collections.sort(cuts);
			Collections.reverse(cuts);

			// Find the corresponding indexes of each float value cut point.
			ArrayList<Integer> cutpoints = new ArrayList<Integer>();
			int wineIndex = wines.size() - 1;
			for (Float c : cuts) {
				
				// Find the last occurence of a cut point, visiting the wines in reverse order.
				for (; wineIndex >= 0; wineIndex--) {
					if (wines.get(wineIndex).valueOf(attr) == c) {
						cutpoints.add(wineIndex);
						break;
					}
				}
			}
			
			// Reverse the newly created list.
			Collections.reverse(cutpoints);
			
			// Save these cuts to the hash map.
			attrCuts.put(attr, cutpoints);

			// Check if the attribute has a greater info gain than the current maximum.
			float tmpInfoGain = getCutInfoGain(cutpoints, wines, initEnt);
			if (tmpInfoGain > maxInfoGain) {
				maxInfoGain = tmpInfoGain;
				bestCutIndex = attr;
			}
		}

		// If none of the attributes is good enough to split, output a leaf.
		if (maxInfoGain == 0) {
			context.write(qu.getQualityPercentages(path), new Text(""));
			return;
		}

		/* Otherwise, sort the list based on the best split attribute, create the extended paths 
		 * and output to the reducer. 
		 */
		Collections.sort(wines, new WineComparator(bestCutIndex));
		for (String p : getNewPaths(path, wines, attrCuts.get(bestCutIndex), bestCutIndex)) {
			if (p.contains("train")) { // first run
				p = p.substring(p.indexOf(',') + 1);
			}
			context.write(new Text(p), content);
		}
	}
}
