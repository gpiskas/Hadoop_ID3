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

package id3;

import java.io.IOException;
import java.util.StringTokenizer;

import main.Main;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import car.CarAttributes;

public class CarReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text thePath, Iterable<Text> file, Context context) throws IOException, InterruptedException {
		String path = thePath.toString();

		// If the path is a leaf, add it to the default reducer output.
		if (path.contains("%")) {
			context.write(thePath, null);
			return;
		}
		
		int equalIndex = path.lastIndexOf('=');
		String attrName = path.substring(path.lastIndexOf(',') + 1, equalIndex);
		String attrValue = path.substring(equalIndex + 1);
		int index = CarAttributes.getAttrIndex(attrName);

		/* Otherwise, create a new data file to be processed in the next phase,
		 * that consists of records of which attribute "attrName" has value "attrValue".
		 */
		FSDataOutputStream out = FileSystem.get(context.getConfiguration())
				.create(new Path(FileOutputFormat.getOutputPath(context).toString() + Main.INPUT_DIR + "/" + path));
		
		StringTokenizer lineTok = new StringTokenizer(file.iterator().next().toString());
		while (lineTok.hasMoreTokens()) {
			String record = lineTok.nextToken();
			StringTokenizer recordTok = new StringTokenizer(record, ",");
			String[] attributes = new String[6];
			for (int i = 0; i < 6; i++) {
				attributes[i] = recordTok.nextToken();
			}
			// If the record attribute "attrName" has value "attrValue", add it to the output file.
			if (attributes[index].equals(attrValue)) {
				out.write((record + " ").getBytes(), 0, record.length()+1);
			}
		}
		out.close();
	}
}