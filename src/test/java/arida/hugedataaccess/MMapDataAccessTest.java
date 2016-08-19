package arida.hugedataaccess;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MMapDataAccessTest {

	private String fileName = "MMapDataAccessTest.mmap";
	private DataAccess dataAccess;
	
	@Before
	public void setUp() {
		dataAccess = new MMapDataAccess(fileName, 3);
		dataAccess.ensureCapacity(11);
	}
	
	@Test
	public void testSize() {
		assertEquals(12, dataAccess.size());
		dataAccess.ensureCapacity(5);
		assertEquals(12, dataAccess.size());
		dataAccess.ensureCapacity(13);
		assertEquals(15, dataAccess.size());
	}

	@Test
	public void testGetBufferIndex() {
		assertEquals(0, ((MMapDataAccess)dataAccess).getBufferIndex(0));
		assertEquals(0, ((MMapDataAccess)dataAccess).getBufferRelativePos(0));
		assertEquals(0, ((MMapDataAccess)dataAccess).getBufferIndex(1));
		assertEquals(1, ((MMapDataAccess)dataAccess).getBufferRelativePos(1));
		assertEquals(0, ((MMapDataAccess)dataAccess).getBufferIndex(2));
		assertEquals(2, ((MMapDataAccess)dataAccess).getBufferRelativePos(2));

		assertEquals(1, ((MMapDataAccess)dataAccess).getBufferIndex(3));
		assertEquals(1, ((MMapDataAccess)dataAccess).getBufferIndex(4));
		assertEquals(1, ((MMapDataAccess)dataAccess).getBufferIndex(5));

		assertEquals(2, ((MMapDataAccess)dataAccess).getBufferIndex(6));
		assertEquals(2, ((MMapDataAccess)dataAccess).getBufferIndex(7));
		assertEquals(2, ((MMapDataAccess)dataAccess).getBufferIndex(8));

		assertEquals(3, ((MMapDataAccess)dataAccess).getBufferIndex(9));
		assertEquals(3, ((MMapDataAccess)dataAccess).getBufferIndex(10));
		assertEquals(3, ((MMapDataAccess)dataAccess).getBufferIndex(11));
	}

	@Test
	public void testGetBufferRelativePos() {
		assertEquals(0, ((MMapDataAccess)dataAccess).getBufferRelativePos(0));
		assertEquals(1, ((MMapDataAccess)dataAccess).getBufferRelativePos(1));
		assertEquals(2, ((MMapDataAccess)dataAccess).getBufferRelativePos(2));
		
		assertEquals(0, ((MMapDataAccess)dataAccess).getBufferRelativePos(3));
		assertEquals(1, ((MMapDataAccess)dataAccess).getBufferRelativePos(4));
		assertEquals(2, ((MMapDataAccess)dataAccess).getBufferRelativePos(5));

		assertEquals(0, ((MMapDataAccess)dataAccess).getBufferRelativePos(6));
		assertEquals(1, ((MMapDataAccess)dataAccess).getBufferRelativePos(7));
		assertEquals(2, ((MMapDataAccess)dataAccess).getBufferRelativePos(8));

		assertEquals(0, ((MMapDataAccess)dataAccess).getBufferRelativePos(9));
		assertEquals(1, ((MMapDataAccess)dataAccess).getBufferRelativePos(10));
		assertEquals(2, ((MMapDataAccess)dataAccess).getBufferRelativePos(11));
	}

	@Test
	public void testGetDouble() {
		dataAccess.addDouble(10.9);
		assertEquals(10.9, dataAccess.getDouble(0), 0.01);
		
	}
	
	
	@After
	public void TearDown() {
		dataAccess.close();
		FileUtils.delete(fileName);
	}
	
}
