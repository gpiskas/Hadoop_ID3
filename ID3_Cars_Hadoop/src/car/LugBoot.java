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

import main.Utils;

// LugBoot attribute for Car.
public class LugBoot {
	
	private int small, med, big,
			    small_unacc, small_acc, small_good, small_vgood,
			    med_unacc, med_acc, med_good, med_vgood, 
			    big_unacc, big_acc, big_good, big_vgood;

	// Checks if the attribute is active.
	private boolean isActive;

	public LugBoot(boolean active) {
		isActive = active;
	}

	/* If the attribute is active, the given value and class are processed,
	 * incrementing the corresponding counters.
	 */
	public void processRecord(String v, String c) {
		if (!isActive) return;
		
		if (v.equals("small")) {
			small += 1;
			addToSmall(c);
		} else if (v.equals("med")) {
			med += 1;
			addToMed(c);
		} else {
			big += 1;
			addToBig(c);
		}
	}

	// Helper function for processRecord.
	private void addToSmall(String c) {
		if (c.equals("unacc")) {
			small_unacc += 1;
		} else if (c.equals("acc")) {
			small_acc += 1;
		} else if (c.equals("good")) {
			small_good += 1;
		} else {
			small_vgood += 1;
		}
	}

	// Helper function for processRecord.
	private void addToMed(String c) {
		if (c.equals("unacc")) {
			med_unacc += 1;
		} else if (c.equals("acc")) {
			med_acc += 1;
		} else if (c.equals("good")) {
			med_good += 1;
		} else {
			med_vgood += 1;
		}
	}

	// Helper function for processRecord.
	private void addToBig(String c) {
		if (c.equals("unacc")) {
			big_unacc += 1;
		} else if (c.equals("acc")) {
			big_acc += 1;
		} else if (c.equals("good")) {
			big_good += 1;
		} else {
			big_vgood += 1;
		}
	}

	// Calculates and returns the info gain of the attribute.
	public float infoGain(float initEnt) {
		if (!isActive) return 0;

		int total = small + med + big;
		return initEnt - (float)small / total * Utils.getEntropy(small_unacc, small_acc, small_good, small_vgood, small)
					   - (float)med / total * Utils.getEntropy(med_unacc, med_acc, med_good, med_vgood, med)
					   - (float)big / total * Utils.getEntropy(big_unacc, big_acc, big_good, big_vgood, big);
	}

	public int getSmall() {
		return small;
	}

	public int getMed() {
		return med;
	}

	public int getBig() {
		return big;
	}	
	
}