/*
* Hadoop ID3
* Copyright (C) 2013 George Piskas, George Oikonomidis
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

package wine;

import java.util.ArrayList;

import org.apache.hadoop.io.Text;

import main.Utils;

// Class that holds quality statistics about a wine list.
public class QualityStats {

	private int[] classes;
	
	// Counts how many different classes are present.
	private int activeClassCount;
	private int wineCount;

	// Given a list of wines, it counts each quality value seperately.
	public QualityStats(ArrayList<Wine> wines) {
		classes = new int[11];
		for (int i = 0; i < 11; i++) {
			classes[i] = 0;
		}
		activeClassCount = 0;
		wineCount = wines.size();
		
		for (Wine w : wines) {
			int c = w.getQu();
			if (classes[c] == 0) {
				activeClassCount += 1;
			}
			classes[c] += 1;
		}
	}

	// Default constructor.
	public QualityStats() {
		classes = new int[11];
		for (int i = 0; i < 11; i++) {
			classes[i] = 0;
		}
		activeClassCount = 0;
		wineCount = 0;
	}

	// Increases a wine quality rating counter.
	public void increment(int rating) {
		if (classes[rating] == 0) {
			activeClassCount += 1;
		}
		classes[rating] += 1;
		wineCount += 1;
	}

	// Decreases a wine quality rating counter.
	public void decrement(int rating) {
		classes[rating] -= 1;
		if (classes[rating] == 0) {
			activeClassCount -= 1;
		}
		wineCount -= 1;
	}

	// Returns the entropy of the quality stats.
	public float getEntropy() {
		float entropy = 0;
		if (wineCount == 0) {
			return 0;
		} else {
			for (int i = 0; i < 11; i++) {
				entropy -= (float) classes[i] / wineCount * Utils.log2((float) classes[i] / wineCount);
			}
		}
		return entropy;
	}

	// Returns a string representation of classification percentages.
	public Text getQualityPercentages(String path) {
		StringBuilder sb = new StringBuilder(path + "_");
		for (int i = 0; i <= 10; i++) {
			sb.append("quality=" + i + ":" + (float) classes[i] * 100 / wineCount + "%, ");
		}
		String s = sb.toString();
		return new Text(s.substring(0, s.length() - 2));
	}

	public int getWineCount() {
		return wineCount;
	}

	public int getActiveClassCount() {
		return activeClassCount;
	}
}
