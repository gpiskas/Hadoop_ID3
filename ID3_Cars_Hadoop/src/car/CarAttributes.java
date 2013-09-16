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

import java.util.ArrayList;

// Class that holds various information about the car attributes.
public class CarAttributes {

	public static final int I_BUYING = 0;
	public static final int I_MAINTENANCE = 1;
	public static final int I_DOORS = 2;
	public static final int I_PERSONS = 3;
	public static final int I_LUGBOOT = 4;
	public static final int I_SAFETY = 5;
	public static final int I_CLASS = 6;

	public static final String S_BUYING = "buying";
	public static final String S_MAINTENANCE = "maintenance";
	public static final String S_DOORS = "doors";
	public static final String S_PERSONS = "persons";
	public static final String S_LUGBOOT = "lugboot";
	public static final String S_SAFETY = "safety";
	public static final String S_CLASS = "class";
	
	private Buying b;
	private Maintenance m;
	private Doors d;
	private Persons p;
	private LugBoot l;
	private Safety s;
	private Classification c;
	
	public CarAttributes(String path) {
		b = new Buying(!path.contains(S_BUYING));
		m = new Maintenance(!path.contains(S_MAINTENANCE));
		d = new Doors(!path.contains(S_DOORS));
		p = new Persons(!path.contains(S_PERSONS));
		l = new LugBoot(!path.contains(S_LUGBOOT));
		s = new Safety(!path.contains(S_SAFETY));
		c = new Classification();
	}

	// Calls the processRecord method for each attribute with the correct values.
	public void processRecord(String[] attributes) {
		b.processRecord(attributes[0], attributes[6]);
		m.processRecord(attributes[1], attributes[6]);
		d.processRecord(attributes[2], attributes[6]);
		p.processRecord(attributes[3], attributes[6]);
		l.processRecord(attributes[4], attributes[6]);
		s.processRecord(attributes[5], attributes[6]);
		c.processRecord(attributes[6]);
	}

	// Returns the best node to split the data.
	private int getBestNodeIndex() {
		float initEnt = c.getInitEntropy();
		
		return maxIndex(new float[] {b.infoGain(initEnt),
									 m.infoGain(initEnt),
									 d.infoGain(initEnt),
									 p.infoGain(initEnt),
									 l.infoGain(initEnt),
									 s.infoGain(initEnt)});
	}
	
	// Returns the index of the attribute with the max info gain.
	private int maxIndex(float[] gain) {
		int maxI = 0;
		float max = gain[0];

		for (int i = 1; i < 6; i++) {
			if (gain[i] > max) {
				max = gain[i];
				maxI = i;
			}
		}
		return maxI;
	}
	
	/* 
	 * Given the best node and the previous path, appends to the current 
	 * path the non-zero attribute values. The result is a list of 
	 * extended paths.
	 */
	public ArrayList<String> getNewPaths(String path) {
		ArrayList<String> paths = new ArrayList<String>();
		// Gets the split attribute.
		switch (getBestNodeIndex()) {
		case I_BUYING:
			// If the attribute value is not 0, create a new extended path.
			if (b.getLow() != 0)
				paths.add(path + "," + S_BUYING + "=low");
			if (b.getMed() != 0)
				paths.add(path + "," + S_BUYING + "=med");
			if (b.getHigh() != 0)
				paths.add(path + "," + S_BUYING + "=high");
			if (b.getVhigh() != 0)
				paths.add(path + "," + S_BUYING + "=vhigh");
			break;
		case I_MAINTENANCE:
			if (m.getLow() != 0)
				paths.add(path + "," + S_MAINTENANCE + "=low");
			if (m.getMed() != 0)
				paths.add(path + "," + S_MAINTENANCE + "=med");
			if (m.getHigh() != 0)
				paths.add(path + "," + S_MAINTENANCE + "=high");
			if (m.getVhigh() != 0)
				paths.add(path + "," + S_MAINTENANCE + "=vhigh");
			break;
		case I_DOORS:
			if (d.get_2() != 0)
				paths.add(path + "," + S_DOORS + "=2");
			if (d.get_3() != 0)
				paths.add(path + "," + S_DOORS + "=3");
			if (d.get_4() != 0)
				paths.add(path + "," + S_DOORS + "=4");
			if (d.get_5more() != 0)
				paths.add(path + "," + S_DOORS + "=5more");
			break;
		case I_PERSONS:
			if (p.get_2() != 0)
				paths.add(path + "," + S_PERSONS + "=2");
			if (p.get_4() != 0)
				paths.add(path + "," + S_PERSONS + "=4");
			if (p.getMore() != 0)
				paths.add(path + "," + S_PERSONS + "=more");
			break;
		case I_LUGBOOT:
			if (l.getSmall() != 0)
				paths.add(path + "," + S_LUGBOOT + "=small");
			if (l.getMed() != 0)
				paths.add(path + "," + S_LUGBOOT + "=med");
			if (l.getBig() != 0)
				paths.add(path + "," + S_LUGBOOT + "=big");
			break;
		case I_SAFETY:
			if (s.getLow() != 0)
				paths.add(path + "," + S_SAFETY + "=low");
			if (s.getMed() != 0)
				paths.add(path + "," + S_SAFETY + "=med");
			if (s.getHigh() != 0)
				paths.add(path + "," + S_SAFETY + "=high");
			break;

		}
		return paths;
	}

	// Given the attrName, returns the corresponding index.
	public static int getAttrIndex(String attrName) {
		if (attrName.equals(S_BUYING))
			return I_BUYING;
		if (attrName.equals(S_MAINTENANCE))
			return I_MAINTENANCE;
		if (attrName.equals(S_DOORS))
			return I_DOORS;
		if (attrName.equals(S_PERSONS))
			return I_PERSONS;
		if (attrName.equals(S_LUGBOOT))
			return I_LUGBOOT;
		if (attrName.equals(S_SAFETY))
			return I_SAFETY;
		if (attrName.equals(S_CLASS))
			return I_CLASS;
		return -1;
	}
}
