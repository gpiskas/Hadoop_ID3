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

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import wine.Wine;

public class WineReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text thePath, Iterable<Text> file, Context context) throws IOException, InterruptedException {
		String path = thePath.toString();

		// If the path is a leaf, add it to the default reducer output.
		if (path.contains("%")) {
			context.write(thePath, null);
			return;
		}

		StringTokenizer pathTok = new StringTokenizer(path.substring(path.lastIndexOf(',') + 1));
		int attrIndex = Wine.indexOf(pathTok.nextToken());
		float from = Float.parseFloat(pathTok.nextToken());
		float to = Float.parseFloat(pathTok.nextToken());

		/* Otherwise, create a new data file to be processed in the next phase,
		 * that consists of records of which attribute "attrIndex" has value between "from" and "to".
		 */
		FSDataOutputStream out = FileSystem.get(context.getConfiguration())
				.create(new Path(FileOutputFormat.getOutputPath(context).toString() + "/input/" + path));
		
		StringTokenizer lineTok = new StringTokenizer(file.iterator().next().toString());
		while (lineTok.hasMoreTokens()) {
			String record = lineTok.nextToken();
			StringTokenizer recordTok = new StringTokenizer(record, ",");
			float[] attributes = new float[11];
			for (int i = 0; i < 11; i++) {
				attributes[i] = Float.parseFloat(recordTok.nextToken());
			}
			
			// If the record attribute "attrIndex" has value in the range (from,to], add it to the output file.
			if (attributes[attrIndex] > from && attributes[attrIndex] <= to) {
				out.write((record + " ").getBytes(), 0, record.length()+1);
			}
		}
		out.close();
	}
}