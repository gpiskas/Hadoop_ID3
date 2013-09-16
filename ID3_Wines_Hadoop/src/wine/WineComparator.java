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

import java.util.Comparator;

// Class that compares two wines based on a given attribute.
public class WineComparator implements Comparator<Wine> {

	int attrIndex;

	public WineComparator(int attr) {
		attrIndex = attr;
	}

	/* Based on the given attribute,
	 * returns 1 if w1 > w2,
	 * returns -1 if w1 < w2,
	 * returns 0 if w1 = w2.
	 */
	@Override
	public int compare(Wine w1, Wine w2) {
		switch (attrIndex) {
		case Wine.I_FI:
			if (w1.getFi() > w2.getFi())
				return 1;
			else if (w1.getFi() < w2.getFi())
				return -1;
			break;
		case Wine.I_VO:
			if (w1.getVo() > w2.getVo())
				return 1;
			else if (w1.getVo() < w2.getVo())
				return -1;
			break;
		case Wine.I_CI:
			if (w1.getCi() > w2.getCi())
				return 1;
			else if (w1.getCi() < w2.getCi())
				return -1;
			break;
		case Wine.I_RE:
			if (w1.getRe() > w2.getRe())
				return 1;
			else if (w1.getRe() < w2.getRe())
				return -1;
			break;
		case Wine.I_CH:
			if (w1.getCh() > w2.getCh())
				return 1;
			else if (w1.getCh() < w2.getCh())
				return -1;
			break;
		case Wine.I_FR:
			if (w1.getFr() > w2.getFr())
				return 1;
			else if (w1.getFr() < w2.getFr())
				return -1;
			break;
		case Wine.I_TO:
			if (w1.getTo() > w2.getTo())
				return 1;
			else if (w1.getTo() < w2.getTo())
				return -1;
			break;
		case Wine.I_DE:
			if (w1.getDe() > w2.getDe())
				return 1;
			else if (w1.getDe() < w2.getDe())
				return -1;
			break;
		case Wine.I_PH:
			if (w1.getPh() > w2.getPh())
				return 1;
			else if (w1.getPh() < w2.getPh())
				return -1;
			break;
		case Wine.I_SU:
			if (w1.getSu() > w2.getSu())
				return 1;
			else if (w1.getSu() < w2.getSu())
				return -1;
			break;
		case Wine.I_AL:
			if (w1.getAl() > w2.getAl())
				return 1;
			else if (w1.getAl() < w2.getAl())
				return -1;
			break;
		case Wine.I_QU:
			if (w1.getQu() > w2.getQu())
				return 1;
			else if (w1.getQu() < w2.getQu())
				return -1;
			break;
		}
		return 0;
	}

}
