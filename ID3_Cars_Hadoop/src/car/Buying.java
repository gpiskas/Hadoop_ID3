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

// Buying attribute of Car.
public class Buying {
	
	private int vhigh, high, med, low,
			    vhigh_unacc, vhigh_acc, vhigh_good, vhigh_vgood,
			    high_unacc, high_acc, high_good, high_vgood, 
			    med_unacc, med_acc, med_good, med_vgood, 
			    low_unacc, low_acc, low_good, low_vgood;
	
	// Checks if the attribute is active.
	private boolean isActive;

	public Buying(boolean active) {
		isActive = active;
	}
	
	/* If the attribute is active, the given value and class are processed,
	 * incrementing the corresponding counters.
	 */
	public void processRecord(String v, String c) {
		if (!isActive) return;
		
		if (v.equals("vhigh")) {
			vhigh += 1;
			addToVhigh(c);
		} else if (v.equals("high")) {
			high += 1;
			addToHigh(c);
		} else if (v.equals("med")) {
			med += 1;
			addToMed(c);
		} else {
			low += 1;
			addToLow(c);
		}
	}

		
	// Helper function for processRecord.
	private void addToVhigh(String c) {
		if (c.equals("unacc")) {
			vhigh_unacc += 1;
		} else if (c.equals("acc")) {
			vhigh_acc += 1;
		} else if (c.equals("good")) {
			vhigh_good += 1;
		} else {
			vhigh_vgood += 1;
		}
	}
	
	// Helper function for processRecord.
	private void addToHigh(String c) {
		if (c.equals("unacc")) {
			high_unacc += 1;
		} else if (c.equals("acc")) {
			high_acc += 1;
		} else if (c.equals("good")) {
			high_good += 1;
		} else {
			high_vgood += 1;
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
	private void addToLow(String c) {
		if (c.equals("unacc")) {
			low_unacc += 1;
		} else if (c.equals("acc")) {
			low_acc += 1;
		} else if (c.equals("good")) {
			low_good += 1;
		} else {
			low_vgood += 1;
		}
	}

	// Calculates and returns the info gain of the attribute.
	public float infoGain(float initEnt) {
		if (!isActive) return 0;

		int total = vhigh + high + med + low;
		return initEnt - (float)vhigh / total * Utils.getEntropy(vhigh_unacc, vhigh_acc, vhigh_good, vhigh_vgood, vhigh)
					   - (float)high / total * Utils.getEntropy(high_unacc, high_acc, high_good, high_vgood, high)
					   - (float)med / total * Utils.getEntropy(med_unacc, med_acc, med_good, med_vgood, med)
					   - (float)low / total * Utils.getEntropy(low_unacc, low_acc, low_good, low_vgood, low);
	}

	public int getVhigh() {
		return vhigh;
	}

	public int getHigh() {
		return high;
	}

	public int getMed() {
		return med;
	}

	public int getLow() {
		return low;
	}
}
