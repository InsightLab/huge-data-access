package hugedataaccess.structures;

public interface MMapMap {
	Long size();
	Long get(Long key);
	void put(Long key, Long value);
	Iterable<Long> keySet();
}
