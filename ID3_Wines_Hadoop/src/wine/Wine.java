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

package wine;

import java.util.ArrayList;

// Class that represent a single wine and its attribute values.
public class Wine {

	private float fi = -1;
	private float vo = -1;
	private float ci = -1;
	private float re = -1;
	private float ch = -1;
	private float fr = -1;
	private float to = -1;
	private float de = -1;
	private float ph = -1;
	private float su = -1;
	private float al = -1;
	private int qu = -1;
	
	public static final String S_FI = "fi";
	public static final String S_VO = "vo";
	public static final String S_CI = "ci";
	public static final String S_RE = "re";
	public static final String S_CH = "ch";
	public static final String S_FR = "fr";
	public static final String S_TO = "to";
	public static final String S_DE = "de";
	public static final String S_PH = "ph";
	public static final String S_SU = "su";
	public static final String S_AL = "al";
	public static final String S_QU = "qu";
	
	public static final int I_FI = 0;
	public static final int I_VO = 1;
	public static final int I_CI = 2;
	public static final int I_RE = 3;
	public static final int I_CH = 4;
	public static final int I_FR = 5;
	public static final int I_TO = 6;
	public static final int I_DE = 7;
	public static final int I_PH = 8;
	public static final int I_SU = 9;
	public static final int I_AL = 10;
	public static final int I_QU = 11;
	
	private String PATH;

	// A list of the active attribute indexes.
	private ArrayList<Integer> active;
	
	public Wine(String path, String[] attributes) {
		PATH = path;
		active = new ArrayList<Integer>();
		processRecord(attributes);
	}

	// Returns the list of active attribute indexes.
	public ArrayList<Integer> getActiveAttributes() {
		return active;
	}

	/* If the attribute is not in the path, assign the 
	 * corresponding value and add it to the "active" list.
	 */
	private void processRecord(String[] attributes) {
		if (!PATH.contains(S_FI)) {
			fi = Float.parseFloat(attributes[0]);
			active.add(I_FI);
		}
		if (!PATH.contains(S_VO)) {
			vo = Float.parseFloat(attributes[1]);
			active.add(I_VO);
		}
		if (!PATH.contains(S_CI)) {
			ci = Float.parseFloat(attributes[2]);
			active.add(I_CI);
		}
		if (!PATH.contains(S_RE)) {
			re = Float.parseFloat(attributes[3]);
			active.add(I_RE);
		}
		if (!PATH.contains(S_CH)) {
			ch = Float.parseFloat(attributes[4]);
			active.add(I_CH);
		}
		if (!PATH.contains(S_FR)) {
			fr = Float.parseFloat(attributes[5]);
			active.add(I_FR);
		}
		if (!PATH.contains(S_TO)) {
			to = Float.parseFloat(attributes[6]);
			active.add(I_TO);
		}
		if (!PATH.contains(S_DE)) {
			de = Float.parseFloat(attributes[7]);
			active.add(I_DE);
		}
		if (!PATH.contains(S_PH)) {
			ph = Float.parseFloat(attributes[8]);
			active.add(I_PH);
		}
		if (!PATH.contains(S_SU)) {
			su = Float.parseFloat(attributes[9]);
			active.add(I_SU);
		}
		if (!PATH.contains(S_AL)) {
			al = Float.parseFloat(attributes[10]);
			active.add(I_AL);
		}
		qu = Integer.parseInt(attributes[11]);
	}

	// Give an attribute name, it returns its index.
	public static int indexOf(String attrName) {
		if (attrName.equals(S_FI))
			return I_FI;
		if (attrName.equals(S_VO))
			return I_VO;
		if (attrName.equals(S_CI))
			return I_CI;
		if (attrName.equals(S_RE))
			return I_RE;
		if (attrName.equals(S_CH))
			return I_CH;
		if (attrName.equals(S_FR))
			return I_FR;
		if (attrName.equals(S_TO))
			return I_TO;
		if (attrName.equals(S_DE))
			return I_DE;
		if (attrName.equals(S_PH))
			return I_PH;
		if (attrName.equals(S_SU))
			return I_SU;
		if (attrName.equals(S_AL))
			return I_AL;
		if (attrName.equals(S_QU))
			return I_QU;
		return -1;
	}

	// Give an attribute index, it returns its name.
	public static String nameOf(int attrIndex) {
		switch (attrIndex) {
		case Wine.I_FI:
			return S_FI;
		case Wine.I_VO:
			return S_VO;
		case Wine.I_CI:
			return S_CI;
		case Wine.I_RE:
			return S_RE;
		case Wine.I_CH:
			return S_CH;
		case Wine.I_FR:
			return S_FR;
		case Wine.I_TO:
			return S_TO;
		case Wine.I_DE:
			return S_DE;
		case Wine.I_PH:
			return S_PH;
		case Wine.I_SU:
			return S_SU;
		case Wine.I_AL:
			return S_AL;
		}
		return null;
	}
	
	// Give an attribute index, it returns its value.
	public float valueOf(int attrIndex) {
		switch (attrIndex) {
		case Wine.I_FI:
			return fi;
		case Wine.I_VO:
			return vo;
		case Wine.I_CI:
			return ci;
		case Wine.I_RE:
			return re;
		case Wine.I_CH:
			return ch;
		case Wine.I_FR:
			return fr;
		case Wine.I_TO:
			return to;
		case Wine.I_DE:
			return de;
		case Wine.I_PH:
			return ph;
		case Wine.I_SU:
			return su;
		case Wine.I_AL:
			return al;
		}
		return -1;
	}

	public float getFi() {
		return fi;
	}

	public float getVo() {
		return vo;
	}

	public float getCi() {
		return ci;
	}

	public float getRe() {
		return re;
	}

	public float getCh() {
		return ch;
	}

	public float getFr() {
		return fr;
	}

	public float getTo() {
		return to;
	}

	public float getDe() {
		return de;
	}

	public float getPh() {
		return ph;
	}

	public float getSu() {
		return su;
	}

	public float getAl() {
		return al;
	}

	public int getQu() {
		return qu;
	}
}
