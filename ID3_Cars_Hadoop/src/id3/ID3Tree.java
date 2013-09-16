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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import car.CarAttributes;
import car.Classification;

import main.Main;

public class ID3Tree {

	private Node root;
	private int fold;

	public static final int TRUE_POSITIVE = 0;
	public static final int FALSE_POSITIVE = 1;
	public static final int FALSE_NEGATIVE = 2;

	// Creates the root node and then, recursively the rest of the tree.
	public ID3Tree(FileSystem fs, int foldNumber) throws IOException {
		BufferedReader input = new BufferedReader(new InputStreamReader(fs.open( new Path(Main.TRAIN_OUTPUT_PATH + Main.FOLD_DIR_PREFIX + foldNumber + Main.LEAVES_PREFIX))));
		String data = input.readLine();
		fold = foldNumber;
		root = new Node(data);
		while ((data = input.readLine()) != null) {
			
			// Creates a node for each leaf in leaves files. 
			root.createNode(data);
		}
		input.close();
	}

	// Once the tree is built, we try to classify the records of the test file.
	public Integer[][] classifyData(FileSystem fs) throws IOException {
		Integer[][] stats = new Integer[4][3];
		for (int i = 0; i < 4; i++) {
			for (int j = 0; j < 3; j++) {
				stats[i][j] = 0;
			}
		}

		BufferedReader input = new BufferedReader(new InputStreamReader(fs.open( new Path(Main.TEST_PATH + Main.TEST_DIR_PREFIX + fold + Main.DATA_TYPE))));

		// Each record is tokenized and saved in "attributes" array, in order to be processed by the tree.
		StringTokenizer lineTok = new StringTokenizer(input.readLine());
		StringTokenizer recordTok;
		String[] attributes = new String[7];
		while (lineTok.hasMoreTokens()) {
			recordTok = new StringTokenizer(lineTok.nextToken(), ",");
			for (int i = 0; i < 7; i++) {
				attributes[i] = recordTok.nextToken();
			}
			// stats[class][TP || FP || FN]
			stats[Classification.getClassIndex(attributes[6])][root.classify(attributes)] += 1;
		}
		input.close();
		return stats;
	}

	/* A tree node with name and children. If the node is a leaf, 
	 * it has no children and holds the class values.
	 */
	private class Node {

		private String nodeName;
		private HashMap<String, Float> classValues;
		private HashMap<String, Node> children;

		public Node(String data) {
			
			// If the node is a leaf then save class values to "classValues" HashMap.
			if (!data.contains("-")) {
				nodeName = CarAttributes.S_CLASS;
				
				classValues = new HashMap<String, Float>();
				StringTokenizer lineTok = new StringTokenizer(data);
				
				classValues.put(Classification.CLASS_UNACC, getClassValue(lineTok.nextToken()));
				classValues.put(Classification.CLASS_ACC, getClassValue(lineTok.nextToken()));
				classValues.put(Classification.CLASS_GOOD, getClassValue(lineTok.nextToken()));
				classValues.put(Classification.CLASS_VGOOD, getClassValue(lineTok.nextToken()));

			// Otherwise, a new node is created, along with its children, recursively.
			} else {
				children = new HashMap<String, Node>();
				createNode(data);
			}
		}

		// Parses the percentage from "classString".
		private float getClassValue(String classString) {
			return Float.valueOf(classString.substring(classString.indexOf(':') + 1, classString.indexOf('%')));
		}
		
		// Recursively creates the path to the leaf, through the children nodes.
		public void createNode(String data) {
			String name, restData;
			
			// If the path contains a single attribute, we use the "dashIndex". Otherwise, we use the "commaIndex".
			int commaIndex = data.indexOf(',');
			int dashIndex = data.indexOf('-');
			if (commaIndex < dashIndex) {
				name = data.substring(0, commaIndex);
				restData = data.substring(commaIndex + 1);
			} else {
				name = data.substring(0, dashIndex);
				restData = data.substring(dashIndex + 1);
			}

			// Parses the name and value of the attribute.
			int equalindex = name.indexOf('=');
			nodeName = name.substring(0, equalindex);
			String value = name.substring(equalindex + 1);

			// If the child node named "value" exists, visit it and recursively create its children.
			if (children.containsKey(value)) {
				Node child = children.get(value);
				child.createNode(restData);
			// Otherwise, create the missing child node. 
			} else {
				children.put(value, new Node(restData));
			}
		}

		/* Processes a single record, top down until a leaf or a missing child is reached.
		 * If a missing child is reached, FALSE_NEGATIVE is returned.
		 * If a leaf is reached and its class percentage is zero, FALSE_POSITIVE is returned.
		 * If a leaf is reached and the percentage is non-zero, TRUE_POSITIVE is returned.
		 */
		public int classify(String[] attributes) {
			int index = CarAttributes.getAttrIndex(nodeName);

			// If the current node is not a leaf node, recursively visit the rest attribute nodes.
			if (index != CarAttributes.I_CLASS) {
				String value = attributes[index];
				if (children.containsKey(value)) {
					return children.get(value).classify(attributes);
				} else {
					return FALSE_NEGATIVE;
				}
			// Otherwise, check the percentages.
			} else if (classValues.get(attributes[index]) != 0) {
				return TRUE_POSITIVE;
			} else {
				return FALSE_POSITIVE;
			}
		}

	}

}
