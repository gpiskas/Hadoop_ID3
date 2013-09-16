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

// Class that represents a range of continuous numbers.
public class Range {

	public float from, to;

	public Range(float from, float to) {
		this.from = from;
		this.to = to;
	}

	// Hash Code is useful for identifying different ranges.
	@Override
	public int hashCode() {
		return (from + "-" + to).hashCode();
	}

	// Tests if two ranges are equal.
	@Override
	public boolean equals(Object obj) {
		if (((Range) obj).getFrom() == from && ((Range) obj).getTo() == to) {
			return true;
		}
		return false;
	}

	public float getFrom() {
		return from;
	}

	public float getTo() {
		return to;
	}

	// Checks if the range contains "num".
	public boolean contains(float num) {
		if (num > from && num <= to) {
			return true;
		}
		return false;
	}
}