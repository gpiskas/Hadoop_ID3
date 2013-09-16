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

// Persons attribute for Car.
public class Persons {
	
	private int _2, _4, more,
			    _2_unacc, _2_acc, _2_good, _2_vgood,
			    _4_unacc, _4_acc, _4_good, _4_vgood, 
			    more_unacc, more_acc, more_good, more_vgood;

	// Checks if the attribute is active.
	private boolean isActive;

	public Persons(boolean active) {
		isActive = active;
	}

	/* If the attribute is active, the given value and class are processed,
	 * incrementing the corresponding counters.
	 */
	public void processRecord(String v, String c){	
		if (!isActive) return;

		if (v.equals("2")) {
			_2 += 1;
			addTo_2(c);
		} else if (v.equals("4")) {
			_4 += 1;
			addTo_4(c);
		} else {
			more += 1;
			addToMore(c);
		}
	}

	// Helper function for processRecord.
	private void addTo_2(String c) {
		if (c.equals("unacc")) {
			_2_unacc += 1;
		} else if (c.equals("acc")) {
			_2_acc += 1;
		} else if (c.equals("good")) {
			_2_good += 1;
		} else {
			_2_vgood += 1;
		}
	}

	// Helper function for processRecord.
	private void addTo_4(String c) {
		if (c.equals("unacc")) {
			_4_unacc += 1;
		} else if (c.equals("acc")) {
			_4_acc += 1;
		} else if (c.equals("good")) {
			_4_good += 1;
		} else {
			_4_vgood += 1;
		}
	}

	// Helper function for processRecord.
	private void addToMore(String c) {
		if (c.equals("unacc")) {
			more_unacc += 1;
		} else if (c.equals("acc")) {
			more_acc += 1;
		} else if (c.equals("good")) {
			more_good += 1;
		} else {
			more_vgood += 1;
		}
	}

	// Calculates and returns the info gain of the attribute.
	public float infoGain(float initEnt) {
		if (!isActive) return 0;
		
		int total = _2 + _4 + more;
		return initEnt - (float)_2 / total * Utils.getEntropy(_2_unacc, _2_acc, _2_good, _2_vgood, _2)
					   - (float)_4 / total * Utils.getEntropy(_4_unacc, _4_acc, _4_good, _4_vgood, _4)
					   - (float)more / total * Utils.getEntropy(more_unacc, more_acc, more_good, more_vgood, more);
	}	
	
	public int get_2() {
		return _2;
	}
	public int get_4() {
		return _4;
	}

	public int getMore() {
		return more;
	}	
	
}