package org.insightlab.hugedataaccess.structures;

import static org.junit.Assert.*;

import java.io.File;

import org.insightlab.hugedataaccess.structures.MMapMap;
import org.insightlab.hugedataaccess.structures.MMapTreeMap;
import org.insightlab.hugedataaccess.util.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class MMapTreeMapTest {
	
	private static MMapMap map;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		String dir = "treeMap/";
		File dirFile = new File(dir);
		if (dirFile.exists()) {
			FileUtils.delete("treeMap/treeMap.mmap");
			FileUtils.delete("treeMap/");
		}
		map = new MMapTreeMap("treeMap/");
		map.put(1l, 4l);
		map.put(4l, 3l);
		map.put(3l, 5l);
		map.put(9l, 1l);
	}
	
	@Test
	public void testContains() {
		assertEquals("Contains Test 1", true, map.containsKey(1l));
		assertEquals("Contains Test 2", false, map.containsKey(20l));
		assertEquals("Contains Test 3", false, map.containsKey(33l));
		assertEquals("Contains Test 4", true, map.containsKey(9l));
	}

	@Test
	public void testSize() {
		long size = map.size();
		map.put(10l, 10l);
		map.put(2l, 2l);
		assertEquals("Size Test 2", size + 2, map.size(), 0);
	}

	@Test
	public void testGet() {
		assertEquals("Get Test 1", 4l, map.get(1l), 0);
		assertEquals("Get Test 2", 3l, map.get(4l), 0);
		assertEquals("Get Test 3", 5l, map.get(3l), 0);
		assertEquals("Get Test 4", 1l, map.get(9l), 0);
	}

	@Test
	public void testPut() {
		map.put(8l, 100l);
		assertEquals("Put Test 1", 100l, map.get(8l), 0);
		map.put(9l, 99l);
		assertEquals("Put Test 2", 99l, map.get(9l), 0);
	}
	
	/*@Test
	public void bigTest() {
		for (long i = 0; i < 100000; i++) {
			map.put(i, i);
		}
		assertEquals("Big test", 82734l, map.get(82734l), 0);
	}*/

}
