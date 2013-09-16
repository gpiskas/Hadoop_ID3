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

package main;

import id3.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

	public static final String TRAIN_INPUT_PATH = "./train/input";
	public static final String TRAIN_OUTPUT_PATH = "./train/output";
	public static final String TEST_PATH = "./test";

	public static final String FOLD_DIR_PREFIX = "/fold_";
	public static final String PHASE_DIR_PREFIX = "/phase_";
	public static final String TRAIN_DIR_PREFIX = "/train_";
	public static final String TEST_DIR_PREFIX = "/test_";
	public static final String LEAVES_PREFIX = "/leaves";
	public static final String INPUT_DIR = "/input";
	public static final String DATA_TYPE = ".data";

	// Creates a new MapReduce job.
	private static Job createJob(Configuration conf) throws IOException {
		Job job = new Job(conf, "CARS");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(CarMapper.class);
		job.setReducerClass(CarReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setJarByClass(main.Main.class);
		return job;
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		// True Positive, False Positive, False Negative counters.
		float totalTP = 0, totalFP = 0, totalFN = 0;
		
		// Clears the output directory before execution.
		fs.delete(new Path(TRAIN_OUTPUT_PATH), true); 
		
		// Ten fold cross validation process.
		for (int foldNumber = 0; foldNumber < 10; foldNumber++) {

			int mrPhase = 0;
			Job job = createJob(conf);
			
			// Sets input and ouput folders for the given fold. 
			FileInputFormat.addInputPath(job, new Path(TRAIN_INPUT_PATH + TRAIN_DIR_PREFIX + foldNumber + DATA_TYPE));
			FileOutputFormat.setOutputPath(job, new Path(TRAIN_OUTPUT_PATH + FOLD_DIR_PREFIX + foldNumber + PHASE_DIR_PREFIX + mrPhase));
			
			// First MapReduce phase begins.
			job.waitForCompletion(true);

			/* If the previous phase provided output to be processed, create and run a new job,
			 * after updating input and output paths.
			 */
			while (fs.exists(new Path(TRAIN_OUTPUT_PATH + FOLD_DIR_PREFIX + foldNumber + PHASE_DIR_PREFIX + mrPhase + INPUT_DIR))) {
				job = createJob(conf);
				FileInputFormat.addInputPath(job, new Path(TRAIN_OUTPUT_PATH + FOLD_DIR_PREFIX + foldNumber + PHASE_DIR_PREFIX + mrPhase + INPUT_DIR));
				mrPhase += 1;
				FileOutputFormat.setOutputPath(job, new Path(TRAIN_OUTPUT_PATH + FOLD_DIR_PREFIX + foldNumber + PHASE_DIR_PREFIX + mrPhase));
				job.waitForCompletion(true);
			}

			// Output leaves are combined in a single file.
			Utils.combineLeaves(fs, foldNumber, mrPhase);

			// Constructs the tree based on the previous leaves file. 
			ID3Tree tree = new ID3Tree(fs, foldNumber);
			
			// Calculates TP, FP, FN for each class, using the test files.
			Integer[][] stats = tree.classifyData(fs);

			// Sums the total TP, FP, FN values of the current fold.
			for (int i = 0; i < 4; i++) {
				totalTP += stats[i][ID3Tree.TRUE_POSITIVE];
				totalFP += stats[i][ID3Tree.FALSE_POSITIVE];
				totalFN += stats[i][ID3Tree.FALSE_NEGATIVE];
			}
		}
		// Calculates precision, recall and f-measure and outputs result. 
		float pre = (float) totalTP / (totalTP + totalFP);
		float rec = (float) totalTP / (totalTP + totalFN);
		float fm = (2 * pre * rec) / (pre + rec);
		System.out.println("Precision :" + pre);
		System.out.println("Recall :" + rec);
		System.out.println("F-Measure :" + fm);
	}
}