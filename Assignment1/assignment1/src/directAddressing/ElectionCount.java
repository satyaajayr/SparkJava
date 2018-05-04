package directAddressing;

/*  The purpose of this class is to create an array 
 * with the indexes as the voterID and the values as the Candidate Id's 
 * By doing this the direct addressing of the voter Id is achieved */

/*  As the voterId is a 6 digit number which ranges from One lakh(100000) to 9,99,999,
 * I tried to reduce the size of the array to 9 lakh instead of 10 lakh
 * by deducting one lakh each time we are adding or accessing the VoterId  */
public class ElectionCount {

	private int[] voterData;
	private int[] candidateData = new int[1000];
	private int reducedBy;
	/* Default size of the voterData array size. */
	private final int SIZE_DEFAULT = 900000;
	// Maintains the value voterId is reduced by to fit the voterData array size.
	private final int REDUCEDBY_DEFAULT = 100000;

	/* Constructor to define default voterId size */
	public ElectionCount() {
		voterData = new int[SIZE_DEFAULT];
		reducedBy = REDUCEDBY_DEFAULT;
	}

	/*
	 * Constructor if the arraySize and the reducedBy values need to be dynamically
	 * passed for the VoterData array
	 */
	public ElectionCount(int arraySize, int reducedBy) {
		voterData = new int[arraySize];
		this.reducedBy = reducedBy;
	}

	/* Method to ADD data to the voterData */
	public void add(int voterId, int candidateID) {
		if (voterId >= 100000 && voterId < 1000000 && candidateID >= 100 && candidateID < 1000) {
			voterId = voterId - reducedBy;
			voterData[voterId] = candidateID;

			/*
			 * if a candidateId is existing in the candidateData array then the count is
			 * incremented by one else the value is initialized to one.
			 */
			if (candidateData[candidateID] == 0) {
				candidateData[candidateID] = 1;
			} else {
				candidateData[candidateID]++;
			}
		}
	}

	/*
	 * Method to FIND data to the voterData, Verifies if the voterId is a 6 digit
	 * number. Returns '0' if not found or out of range.
	 */
	public int find(int voterId) {
		if (voterId >= 100000 && voterId < 1000000) {
			voterId = voterId - reducedBy;
			return voterData[voterId];
		} else {
			return 0;
		}
	}

	/*
	 * Method to retrieve the COUNT of candidatId, if it is a 3 digit number from
	 * candidateData array.
	 */
	public int count(int candidateID) {
		int totalCount = 0;
		if (candidateID >= 100 && candidateID < 1000) {
			totalCount = candidateData[candidateID];
		}
		return totalCount;
	}

}
