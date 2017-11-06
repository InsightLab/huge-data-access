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
				treeAccess.setLong(8, -1);
				pos = 0;
				root = -1;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Long size() {
		return treeAccess.getLong(0);
	}

	public Long get(Long key) {
		long node = root;
		while (node != -1l) {
			long nodeKey = getKey(node);
			if (nodeKey == key) return getValue(node);
			else if (key < nodeKey) node = getLeft(node);
			else node = getRight(node);
		}
		return null;
	}

	public void put(Long key, Long value) {
		if ((size() + 1)*NODE_SIZE > treeAccess.getCapacity()) treeAccess.ensureCapacity(treeAccess.getCapacity() + 1024*1024l);
		if (root == -1) {
			root = pos;
			setKey(root, key);
			setValue(root, value);
			setLeft(root, -1);
			setRight(root, -1);
			setParent(root, -1);
			setHeight(root, 1);
			treeAccess.setLong(0, treeAccess.getLong(0) + 1);
			treeAccess.setLong(8, root);
			pos++;
		}
		else {
			long child = root;
			long parent = child;
			boolean isLeft = true;
			while(child != -1) {
				parent = child;
				if (getKey(parent) == key) {
					setValue(parent, value);
					return;
				}
				else if (key < getKey(parent)) {
					child = getLeft(parent);
					isLeft = true;
				}
				else {
					child = getRight(parent);
					isLeft = false;
				}
			}
			setKey(pos, key); 
			setValue(pos, value);
			setLeft(pos, -1);
			setRight(pos, -1);
			setParent(pos, parent);
			setHeight(pos, 1);
			treeAccess.setLong(0, treeAccess.getLong(0) + 1);
			if (isLeft) setLeft(parent, pos);
			else setRight(parent, pos);
			fixHeights(pos);
			pos++;
		}	
	}
	
	private void fixHeights(long start) {
		long parent = getParent(start);
		while(parent != -1) {
			setHeight(parent, Math.max(getHeight(getLeft(parent)), getHeight(getRight(parent))) + 1);
			parent = getParent(parent);
		}
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
		if (pos == -1) return 0;
		return treeAccess.getInt(getIndex(pos) + 8*5);
	}

	private void setKey(long pos, long key) {
		treeAccess.setLong(getIndex(pos), key);
	}
	
	private void setValue(long pos, long value) {
		treeAccess.setLong(getIndex(pos) + 8, value);
	}
	
	private void setLeft(long pos, long left) {
		treeAccess.setLong(getIndex(pos) + 8*2, left);
	}
	
	
	private void setRight(long pos, long right) {
		treeAccess.setLong(getIndex(pos) + 8*3, right);
	}
	
	private void setParent(long pos, long parent) {
		treeAccess.setLong(getIndex(pos) + 8*4, parent);
	}
	
	private void setHeight(long pos, int height) {
		treeAccess.setInt(getIndex(pos) + 8*5, height);
	}
	
	public void printOrder(Long pos) {
		if (pos != -1) {
			printOrder(getLeft(pos));
			System.out.println(getKey(pos));
			printOrder(getRight(pos));
		}
	}

}
