package hugedataaccess.structures;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import hugedataaccess.DataAccess;
import hugedataaccess.MMapDataAccess;

public class MMapTreeMap implements MMapMap {
	
	private static final int NODE_SIZE = 5*8 + 4;
	private DataAccess treeAccess;
	private long pos = 0;
	private long root = -1;
	
	/* 
	 * size : long
	 * root : long
	 *
	 */
	
	/* 
	 * key : long
	 * value : long
	 * left : long
	 * right : long
	 * parent : long
	 * height : int
	 * 
	 */
	
	public MMapTreeMap(String path) {
		if (!path.endsWith("/")) path += "/";
		File f = new File(path);
		boolean exists = true;
		if (!f.exists()) {
			f.mkdirs();
			exists = false;
		}
		try {
			treeAccess = new MMapDataAccess(path + "treeMap.mmap", 1024*1024l);
			if (exists) {
				pos = treeAccess.getLong(0);
				root = treeAccess.getLong(8);
			}
			else {
				treeAccess.setLong(0, 0);
				treeAccess.setLong(8, 0);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private long getIndex(long pos) {
		return 16 + pos * NODE_SIZE;
	}
	
	private long getKey(long pos) {
		return treeAccess.getLong(getIndex(pos));
	}
	
	private long getValue(long pos) {
		return treeAccess.getLong(getIndex(pos) + 8);
	}
	
	private long getLeft(long pos) {
		return treeAccess.getLong(getIndex(pos) + 8*2);
	}
	
	
	private long getRight(long pos) {
		return treeAccess.getLong(getIndex(pos) + 8*3);
	}
	
	private long getParent(long pos) {
		return treeAccess.getLong(getIndex(pos) + 8*4);
	}
	
	private int getHeight(long pos) {
		return treeAccess.getInt(getIndex(pos) + 8*5);
	}

	public Long size() {
		return treeAccess.getLong(0);
	}

	public Long get(Long key) {
		long node = root;
		while (node != -1) {
			long nodeKey = getKey(node);
			if (nodeKey == key) return getValue(key);
			else if (nodeKey < key) node = getLeft(node);
			else node = getRight(node);
		}
		return null;
	}

	public void put(Long key, Long value) {
		// TODO Auto-generated method stub

	}

	public Iterable<Long> keySet() {
		return new Iterable<Long>() {
			public Iterator<Long> iterator() {
				return new Iterator<Long>() {
					
					private long i = 0;

					public boolean hasNext() {
						return i < size();
					}

					public Long next() {
						return getKey(i++);
					}
				};
			}
		};
	}

}
