package hashTable;

import java.util.*;
import java.io.File;
import java.io.IOException;

public class Source {

	public static void main(String[] args) throws Exception {

		/* Please mention the data file path here */
		String fileName = 
				"C:\\Users\\satya\\eclipse-workspace\\Initial\\assignment1\\src\\data\\data.txt";

		/* Declaring the ElectionCount class to create an voterId Array */
		ElectionCount eCount = new ElectionCount();

		/*
		 * This method will in turn call the add method in ElectionCount class. This
		 * method is a static method in Source class written to validate and add data to
		 * the VoterData Array.
		 */
		addVoterData(fileName, eCount);

		/* Find functionality */
		System.out.println(eCount.find(151027));
		System.out.println(eCount.find(151024));
		System.out.println(eCount.find(151089));
		System.out.println(eCount.find(189657));
		System.out.println(eCount.find(269856));
		System.out.println(eCount.find(0));

		/* Count functionality */
		System.out.println(eCount.count(135));
		System.out.println(eCount.count(130));
		System.out.println(eCount.count(132));
		System.out.println(eCount.count(145));
		System.out.println(eCount.count(0));

	}

	/*
	 * This method is used to read the file with the specified filename and adds the
	 * voter data to the ElectionCount Class array voterData[].
	 */
	public static void addVoterData(String fileName, ElectionCount eCount) {

		String line = "";
		int v = 0, c = 0; // voterId and CandidateId are initialized to zero.
		final int SPLIT_SIZE = 2;
		String[] split = new String[SPLIT_SIZE];

		/*
		 * Reading the file using a Scanner and building the voterData array in
		 * ElectionCount class
		 */
		try (Scanner scanner = new Scanner(new File(fileName))) {
			while (scanner.hasNext()) {
				line = scanner.nextLine();
				split = line.split("\\s+");
				// validation if either voterId or candidatId are not present
				if (split.length < 2) { 
					System.out.println("Skipped adding to array: "
							+ "Invalid Row in input File:" + line);
					continue;
				}

				try { // validation if expected voterId is not passed
					v = Integer.parseInt(split[0]);
				} catch (NumberFormatException nfe) {
					System.out.println("Skipped adding to array: "
							+ "Invalid VoterId in input File:" + line);
					continue;
				}

				try { // validation if expected CandidateId is not passed
					c = Integer.parseInt(split[1]);
				} catch (NumberFormatException nfe) {
					System.out.println("Skipped adding to array: "
							+ "Invalid CandidateId in input File:" + line);
					continue;
				}

				if (v >= 100000 && v < 1000000 && c >= 100 && c < 1000) {
					eCount.add(v, c);
				} else {
					System.out.println("Skipped adding to array: "
							+ "Invalid VoterId : " + v + " or CandidateId : " + c);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
