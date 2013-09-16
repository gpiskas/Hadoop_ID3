/*
* Hadoop ID3
* Copyright (C) 2013 George Piskas, George Economides
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
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import car.CarAttributes;
import car.Classification;

public class CarMapper extends Mapper<LongWritable, Text, Text, Text> {

	
	private boolean isLeaf(String path, String records) {
		
		// Checks if all attributes have been used as split points so far.
		if (path.contains(CarAttributes.S_BUYING)
				&& path.contains(CarAttributes.S_MAINTENANCE)
				&& path.contains(CarAttributes.S_DOORS)
				&& path.contains(CarAttributes.S_PERSONS)
				&& path.contains(CarAttributes.S_LUGBOOT)
				&& path.contains(CarAttributes.S_SAFETY)) {// all checked
			return true;
		}

		// Checks if all current records belong to the same class.
		boolean allSameClass = false;
		int spaceIndex = records.indexOf(' ');
		String firstRecord = records.substring(0, spaceIndex);
		String restRecords = records.substring(spaceIndex + 1);
		String cls = firstRecord.substring(firstRecord.lastIndexOf(',') + 1);

		/* The first record has class = "unacc" and the rest of the records 
		 * do not contain "acc","good" or "vgood".
		 */
		if (cls.equals(Classification.CLASS_UNACC)) {
			allSameClass = !restRecords.contains("," + Classification.CLASS_ACC + " ")
					&& !restRecords.contains("," + Classification.CLASS_GOOD + " ")
					&& !restRecords.contains("," + Classification.CLASS_VGOOD + " ");
		} else if (cls.equals(Classification.CLASS_ACC)) {
			allSameClass = !restRecords.contains("," + Classification.CLASS_UNACC + " ")
					&& !restRecords.contains("," + Classification.CLASS_GOOD + " ")
					&& !restRecords.contains("," + Classification.CLASS_VGOOD + " ");
		} else if (cls.equals(Classification.CLASS_GOOD)) {
			allSameClass = !restRecords.contains("," + Classification.CLASS_UNACC + " ")
					&& !restRecords.contains("," + Classification.CLASS_ACC + " ")
					&& !restRecords.contains("," + Classification.CLASS_VGOOD + " ");
		} else {
			allSameClass = !restRecords.contains("," + Classification.CLASS_UNACC + " ")
					&& !restRecords.contains("," + Classification.CLASS_ACC	+ " ")
					&& !restRecords.contains("," + Classification.CLASS_GOOD + " ");
		}
		return allSameClass;
	}

	public void map(LongWritable key, Text content, Context context) throws IOException, InterruptedException {
		// Input filename - path so far.
		String path = ((FileSplit) context.getInputSplit()).getPath().getName();
		String records = content.toString();

		/* If the path is a leaf, count class percentages and append them to the current path.
		 * Then, a key-value pair that consists of key = path, value = empty file.
		 */
		if (isLeaf(path, records)) {
			context.write(Classification.getClassPercentages(path, records), new Text(""));
			return;
		}

		CarAttributes carAttr = new CarAttributes(path);

		// Iterates over each record and increments the corresponding counter.
		StringTokenizer lineTok = new StringTokenizer(records);
		StringTokenizer recordTok;
		String[] attributes = new String[7];
		while (lineTok.hasMoreTokens()) {
			recordTok = new StringTokenizer(lineTok.nextToken(), ",");
			for (int i = 0; i < 7; i++) {
				attributes[i] = recordTok.nextToken();
			}
			carAttr.processRecord(attributes);
		}

		// Outputs the extended paths that were created by the split.
		for (String p : carAttr.getNewPaths(path)) {
			if (p.contains("train")) { // first run
				p = p.substring(p.indexOf(',') + 1);
			}
			context.write(new Text(p), content);
		}
	}
}
