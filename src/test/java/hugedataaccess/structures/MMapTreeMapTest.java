package hugedataaccess.structures;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;

import hugedataaccess.util.FileUtils;

public class MMapTreeMapTest {
	
	private static MMapMap map;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		FileUtils.delete("/treeMap/treeMap.mmap");
		FileUtils.delete("/treeMap/");
		map = new MMapTreeMap("/treeMap/");
//		System.out.println("putting 1");
		map.put(1l, 4l);
//		System.out.println("putting 4");
		map.put(4l, 3l);
//		System.out.println("putting 3");
		map.put(3l, 5l);
//		System.out.println("putting 9");
		map.put(9l, 1l);
		//map.printOrder(0l);
	}

	@Test
	public void testSize() {
		long size = map.size();
//		System.out.println("putting 10");
		map.put(10l, 10l);
//		System.out.println("putting 2");
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
//		System.out.println("putting 8");
		map.put(8l, 100l);
		assertEquals("Put Test 1", 100l, map.get(8l), 0);
//		System.out.println("putting 9");
		map.put(9l, 99l);
		assertEquals("Put Test 2", 99l, map.get(9l), 0);
	}

}
