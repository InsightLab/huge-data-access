package arida.hugedataaccess;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MMapDataAccessTest {

	private static String fileName = "MMapDataAccessTest.mmap";
	private DataAccess dataAccess1;

	@BeforeClass
	public static void setUpClass() {
		FileUtils.delete(fileName);
	}

	
	@Before
	public void setUp() {
		dataAccess1 = new MMapDataAccess(fileName, 21);
		dataAccess1.ensureCapacity(63);
	}
	
	@Test
	public void testSize() {
		assertEquals(63, dataAccess1.getCapacity());
		dataAccess1.ensureCapacity(21);
		assertEquals(63, dataAccess1.getCapacity());
		dataAccess1.ensureCapacity(105);
		assertEquals(105, dataAccess1.getCapacity());
		dataAccess1.ensureCapacity(126);
		assertEquals(126, dataAccess1.getCapacity());
	}

	@Test
	public void testSetGet() {
		dataAccess1.setByte(0, Byte.MAX_VALUE); 		// size = 1
		dataAccess1.setByte(1, (byte)1); 			// size = 1
		dataAccess1.setChar(2, Character.MAX_VALUE);	// size = 2
		dataAccess1.setByte(4, (byte)2); 			// size = 1
		dataAccess1.setShort(5, Short.MAX_VALUE);	// size = 2
		dataAccess1.setByte(7, (byte)3); 			// size = 1
		dataAccess1.setInt(8, Integer.MAX_VALUE);	// size = 4
		dataAccess1.setByte(12, (byte)4); 			// size = 1
		dataAccess1.setLong(13, Long.MAX_VALUE);		// size = 8
		dataAccess1.setByte(21, (byte)5); 			// size = 1
		dataAccess1.setFloat(22, Float.MAX_VALUE);	// size = 4
		dataAccess1.setByte(26, (byte)6); 			// size = 1
		dataAccess1.setDouble(27, Double.MAX_VALUE);	// size = 8
		dataAccess1.setByte(35, (byte)7); 			// size = 1
		dataAccess1.setByte(62, (byte)8); 			// size = 1
		assertEquals(Byte.MAX_VALUE, dataAccess1.getByte(0));
		assertEquals((byte)1, dataAccess1.getByte(1));
		assertEquals(Character.MAX_VALUE, dataAccess1.getChar(2));
		assertEquals((byte)2, dataAccess1.getByte(4));
		assertEquals(Short.MAX_VALUE, dataAccess1.getShort(5));
		assertEquals((byte)3, dataAccess1.getByte(7));
		assertEquals(Integer.MAX_VALUE, dataAccess1.getInt(8));
		assertEquals((byte)4, dataAccess1.getByte(12));
		assertEquals(Long.MAX_VALUE, dataAccess1.getLong(13));
		assertEquals((byte)5, dataAccess1.getByte(21));
		assertEquals(Float.MAX_VALUE, dataAccess1.getFloat(22), 0.0001);
		assertEquals((byte)6, dataAccess1.getByte(26));
		assertEquals(Double.MAX_VALUE, dataAccess1.getDouble(27), 0.0001);
		assertEquals((byte)7, dataAccess1.getByte(35));
		assertEquals((byte)8, dataAccess1.getByte(62));
	}
	
	@After
	public void TearDown() {
		dataAccess1.close();
		FileUtils.delete(fileName);
	}
	
}
