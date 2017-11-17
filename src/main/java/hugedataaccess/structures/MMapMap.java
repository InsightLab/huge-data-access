package hugedataaccess.structures;

public interface MMapMap {
	Long size();
	Long get(Long key);
	void put(Long key, Long value);
	boolean containsKey(Long key);
	Iterable<Long> keySet();
	void printOrder(Long pos);
}
