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
			setBalanceFactor(root, 0);
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
			setBalanceFactor(pos, 0);
			treeAccess.setLong(0, treeAccess.getLong(0) + 1);
			if (isLeft) setLeft(parent, pos);
			else setRight(parent, pos);
			balanceTree(pos);
			pos++;
		}
	}
	
	private void balanceTree(long Z) {
		long G, N = -1;
		for (long X = getParent(Z); X != -1; X = getParent(Z)) {
			if (Z == getRight(X)) {
				if (getBalanceFactor(X) > 0) {
					G = getParent(X);
					if (getBalanceFactor(Z) < 0)
						N = rotateRightLeft(X, Z);
					else
						N = rotateLeft(X, Z);
				} else {
					if (getBalanceFactor(X) < 0) {
						setBalanceFactor(X, 0);
						break;
					}
					setBalanceFactor(X, 1);
					Z = X;
					continue;
				}
			} else {
				if (getBalanceFactor(X) < 0) {
					G = getParent(X);
					if (getBalanceFactor(Z) > 0)
						N = rotateLeftRight(X, Z);
					else
						N = rotateRight(X, Z);
				} else {
					if (getBalanceFactor(X) > 0) {
						setBalanceFactor(X, 0);
						break;
					}
					setBalanceFactor(X, -1);
					Z = X;
					continue;
				}
			}
			
			setParent(N, G);
			if (G != -1l) {
				if (X == getLeft(G))
					setLeft(G, N);
				else
					setRight(G, N);
			} else
				root = N;
			break;
		}
	}
	
	private long rotateLeft(long X, long Z) {
		//System.out.println("LEFT: " + Z + " to " + X);
		long t23 = getLeft(Z);
		setRight(X, t23);
		if (t23 != -1l)
			setParent(t23, X);
		setLeft(Z, X);
		setParent(X, Z);
		if (getBalanceFactor(Z) == 0) {
			setBalanceFactor(X, 1);
			setBalanceFactor(Z, -1);
		} else {
			setBalanceFactor(X, 0);
			setBalanceFactor(Z, 0);
		}
		return Z;
	}
	
	private long rotateRightLeft(long X, long Z) {
		long Y = getLeft(Z);
		//System.out.println("RIGHT and LEFT: " + Y + " to " + Z + " and " + Y + " to " + X);
		long t3 = getRight(Y);
		setLeft(Z, t3);
		if (t3 != -1l)
			setParent(t3, Z);
		setRight(Y, Z);
		setParent(Z, Y);
		long t2 = getLeft(Y);
		setRight(X, t2);
		if (t2 != -1l)
			setParent(t2, X);
		setLeft(Y, X);
		setParent(X, Y);
		int yBalance = getBalanceFactor(Y);
		if (yBalance > 0) {
			setBalanceFactor(X, -1);
			setBalanceFactor(Z, 0);
		} else if(yBalance == 0) {
			setBalanceFactor(X, 0);
			setBalanceFactor(Z, 0);
		} else {
			setBalanceFactor(X, 0);
			setBalanceFactor(Z, 1);
		}
		setBalanceFactor(Y, 0);
		return Y;
	}
	
	private long rotateRight(long X, long Z) {
		//System.out.println("RIGHT: " + Z + " to " + X);
		long t23 = getRight(Z);
		setLeft(X, t23);
		if (t23 != -1l)
			setParent(t23, X);
		setRight(Z, X);
		setParent(X, Z);
		if (getBalanceFactor(Z) == 0) {
			setBalanceFactor(X, -1);
			setBalanceFactor(Z, 1);
		} else {
			setBalanceFactor(X, 0);
			setBalanceFactor(Z, 0);
		}
		return Z;
	}
	
	private long rotateLeftRight(long X, long Z) {
		long Y = getRight(Z);
		//System.out.println("LEFT and RIGHT: " + Y + " to " + Z + " and " + Y + " to " + X);
		long t3 = getLeft(Y);
		setRight(Z, t3);
		if (t3 != -1l)
			setParent(t3, Z);
		setLeft(Y, Z);
		setParent(Z, Y);
		long t2 = getRight(Y);
		setLeft(X, t2);
		if (t2 != -1l)
			setParent(t2, X);
		setRight(Y, X);
		setParent(X, Y);
		int yBalance = getBalanceFactor(Y);
		if (yBalance < 0) {
			setBalanceFactor(X, 1);
			setBalanceFactor(Z, 0);
		} else if(yBalance == 0) {
			setBalanceFactor(X, 0);
			setBalanceFactor(Z, 0);
		} else {
			setBalanceFactor(X, 0);
			setBalanceFactor(Z, -1);
		}
		setBalanceFactor(Y, 0);
		return Y;
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
	
	private int getBalanceFactor(long pos) {
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
	
	private void setBalanceFactor(long pos, int balanceFactor) {
		treeAccess.setInt(getIndex(pos) + 8*5, balanceFactor);
	}
	
	public void printOrder(Long pos) {
		if (pos != -1) {
			printOrder(getLeft(pos));
			System.out.print(getKey(pos) + " | ");
			printOrder(getRight(pos));
		}
	}

}
