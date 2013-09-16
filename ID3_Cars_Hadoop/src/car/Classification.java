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

package car;

import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

import main.Utils;

// Car classification.
public class Classification {

	private int unacc, acc, good, vgood;

	public static final String CLASS_UNACC = "unacc";
	public static final String CLASS_ACC = "acc";
	public static final String CLASS_GOOD = "good";
	public static final String CLASS_VGOOD = "vgood";
	
	public static final int I_CLASS_UNACC = 0;
	public static final int I_CLASS_ACC = 1;
	public static final int I_CLASS_GOOD = 2;
	public static final int I_CLASS_VGOOD = 3;
	
	// Processes the given class and increments counters.
	public void processRecord(String v) {
		if (v.equals("unacc")) {
			unacc += 1;
		} else if (v.equals("acc")) {
			acc += 1;
		} else if (v.equals("good")) {
			good += 1;
		} else {
			vgood += 1;
		}
	}

	/* Counts the instances of each class and returns a string represantation of
	 * classification percentages.
	 */
	public static Text getClassPercentages(String path, String content) {
		int[] classes = new int[4];
		int total = 0;
					
		StringTokenizer lineTok = new StringTokenizer(content);
		String record = "";
		while (lineTok.hasMoreTokens()) {
			record = lineTok.nextToken(); 
			// Iterates over the String content, incrementing the corresponding class counters.
			classes[getClassIndex(record.substring(record.lastIndexOf(',')+1))] += 1;
		}
		total = classes[0] + classes[1] + classes[2] + classes[3];		
		return new Text(path + "-class=" + CLASS_UNACC + ":" + classes[0]*100/total 
						     + "%, class=" + CLASS_ACC + ":" + classes[1]*100/total
						     + "%, class=" + CLASS_GOOD + ":" + classes[2]*100/total
						     + "%, class=" + CLASS_VGOOD + ":" + classes[3]*100/total + "%");
	}
	
	// Given the classValue, returns the corresponding index.
	public static int getClassIndex(String classValue) {
		if (classValue.equals(CLASS_UNACC))
			return I_CLASS_UNACC;
		if (classValue.equals(CLASS_ACC))
			return I_CLASS_ACC;
		if (classValue.equals(CLASS_GOOD))
			return I_CLASS_GOOD;
		if (classValue.equals(CLASS_VGOOD))
			return I_CLASS_VGOOD;
		return -1;
	}
	
	// Returns classification entropy.
	public float getInitEntropy() {
		return Utils.getEntropy(unacc, acc, good, vgood, unacc + acc + good + vgood);
	}
}