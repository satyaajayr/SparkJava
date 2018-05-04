package hashTable;

import java.util.Hashtable;

/*  The purpose of this class is to create a HashTable Data Structure in Java 
 * It uses two HashTables, One to store voterId and CandidateId
 * the other to store candidateId and the count of the candidateId */
public class ElectionCount {

	Hashtable<Integer, Integer> voterData = new Hashtable<Integer, Integer>();
	Hashtable<Integer, Integer> candidateData = new Hashtable<Integer, Integer>();

	/* Method to ADD data to the voterData HashTable */
	public void add(int voterId, int candidateID) {
		int candidateCount = 0;
		if (voterId >= 100000 && voterId < 1000000 && candidateID >= 100 && candidateID < 1000) {
			voterData.put(voterId, candidateID); // adds record to VoterData HashTable.

			// Adding the data to candidateData HashTable to store the counts of each
			// candidate Id.
			if (candidateData.containsKey(candidateID)) {
				candidateCount = candidateData.get(candidateID);
				candidateData.put(candidateID, candidateCount + 1);
			} else {
				candidateData.put(candidateID, 1);
			}
		}
	}

	/*
	 * Method to FIND data to the voterData. Verifies if the voterId is a 6 digit
	 * number and returns 0 if the voterId is our of range or absent in the Hash Table.
	 */
	public int find(int voterId) {
		if (voterId >= 100000 && voterId < 1000000) {
			return voterData.containsKey(voterId) ? voterData.get(voterId) : 0;
		} else {
			return 0;
		}
	}

	/*
	 * Method to COUNT data to the voterData verifies if the candidatId is a 3 digit
	 * number and retrieves the data from candidateData HashTable.
	 */
	public int count(int candidateID) {
		int totalCount = 0;
		if (candidateID >= 100 && candidateID < 1000) {
			totalCount = candidateData.containsKey(candidateID) ? 
					candidateData.get(candidateID) : totalCount;
		}
		return totalCount;
	}

}
