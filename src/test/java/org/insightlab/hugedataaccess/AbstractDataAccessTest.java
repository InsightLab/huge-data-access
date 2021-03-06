package org.insightlab.hugedataaccess;

import static org.junit.Assert.*;

import org.insightlab.hugedataaccess.ByteBufferDataAccess;
import org.insightlab.hugedataaccess.DataAccess;
import org.insightlab.hugedataaccess.DataAccessException;
import org.junit.Test;

public abstract class AbstractDataAccessTest {

	protected DataAccess dataAccess1;
	protected DataAccess dataAccess2;

	@Test(expected = DataAccessException.class)
	public void testBufferSize() {
		new ByteBufferDataAccess(24);
	}
	
	@Test
	public void testSize() {
		assertEquals(64, dataAccess1.getCapacity());
		dataAccess1.ensureCapacity(64);
		assertEquals(64, dataAccess1.getCapacity());
		dataAccess1.ensureCapacity(192);
		assertEquals(192, dataAccess1.getCapacity());
		dataAccess1.ensureCapacity(320);
		assertEquals(320, dataAccess1.getCapacity());
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
		dataAccess1.setInt(12, (byte)4); 			// size = 4
		dataAccess1.setByte(16, (byte)4); 			// size = 1
		dataAccess1.setLong(17, Long.MAX_VALUE);		// size = 8
		dataAccess1.setByte(25, (byte)5); 			// size = 1
		dataAccess1.setFloat(26, Float.MAX_VALUE);	// size = 4
		dataAccess1.setByte(30, (byte)6); 			// size = 1
		dataAccess1.setByte(31, (byte)6); 			// size = 1
		dataAccess1.setDouble(32, Double.MAX_VALUE);	// size = 8
		dataAccess1.setByte(40, (byte)7); 			// size = 1
		dataAccess1.setByte(63, (byte)8); 			// size = 1
		assertEquals(Byte.MAX_VALUE, dataAccess1.getByte(0));
		assertEquals((byte)1, dataAccess1.getByte(1));
		assertEquals(Character.MAX_VALUE, dataAccess1.getChar(2));
		assertEquals((byte)2, dataAccess1.getByte(4));
		assertEquals(Short.MAX_VALUE, dataAccess1.getShort(5));
		assertEquals((byte)3, dataAccess1.getByte(7));
		assertEquals(Integer.MAX_VALUE, dataAccess1.getInt(8));
		assertEquals((byte)4, dataAccess1.getInt(12));
		assertEquals((byte)4, dataAccess1.getByte(16));
		assertEquals(Long.MAX_VALUE, dataAccess1.getLong(17));
		assertEquals((byte)5, dataAccess1.getByte(25));
		assertEquals(Float.MAX_VALUE, dataAccess1.getFloat(26), 0.0001);
		assertEquals((byte)6, dataAccess1.getByte(30));
		assertEquals((byte)6, dataAccess1.getByte(31));
		assertEquals(Double.MAX_VALUE, dataAccess1.getDouble(32), 0.0001);
		assertEquals((byte)7, dataAccess1.getByte(40));
		assertEquals((byte)8, dataAccess1.getByte(63));
	}
	
	@Test
	public void testSetGet2() {
		dataAccess1.setByte(Byte.MAX_VALUE); 		// size = 1
		dataAccess1.setByte((byte)1); 			// size = 1
		dataAccess1.setChar(Character.MAX_VALUE);	// size = 2
		dataAccess1.setByte((byte)2); 			// size = 1
		dataAccess1.setShort(Short.MAX_VALUE);	// size = 2
		dataAccess1.setByte((byte)3); 			// size = 1
		dataAccess1.setInt(Integer.MAX_VALUE);	// size = 4
		dataAccess1.setInt((byte)4); 			// size = 4
		dataAccess1.setByte((byte)4); 			// size = 1
		dataAccess1.setLong(Long.MAX_VALUE);		// size = 8
		dataAccess1.setByte((byte)5); 			// size = 1
		dataAccess1.setFloat(Float.MAX_VALUE);	// size = 4
		dataAccess1.setByte((byte)6); 			// size = 1
		dataAccess1.setByte((byte)6); 			// size = 1
		dataAccess1.setDouble(Double.MAX_VALUE);	// size = 8
		dataAccess1.setByte((byte)7); 			// size = 1
		dataAccess1.setByte((byte)8); 			// size = 1
		dataAccess1.setCurrentPosition(0);
		assertEquals(Byte.MAX_VALUE, dataAccess1.getByte());
		assertEquals((byte)1, dataAccess1.getByte());
		assertEquals(Character.MAX_VALUE, dataAccess1.getChar());
		assertEquals((byte)2, dataAccess1.getByte());
		assertEquals(Short.MAX_VALUE, dataAccess1.getShort());
		assertEquals((byte)3, dataAccess1.getByte());
		assertEquals(Integer.MAX_VALUE, dataAccess1.getInt());
		assertEquals((byte)4, dataAccess1.getInt());
		assertEquals((byte)4, dataAccess1.getByte());
		assertEquals(Long.MAX_VALUE, dataAccess1.getLong());
		assertEquals((byte)5, dataAccess1.getByte());
		assertEquals(Float.MAX_VALUE, dataAccess1.getFloat(), 0.0001);
		assertEquals((byte)6, dataAccess1.getByte());
		assertEquals((byte)6, dataAccess1.getByte());
		assertEquals(Double.MAX_VALUE, dataAccess1.getDouble(), 0.0001);
		assertEquals((byte)7, dataAccess1.getByte());
		assertEquals((byte)8, dataAccess1.getByte());
	}
	
	@Test
	public void testSetGet3() {
		int sum1 = 0;
		int numberOfElements = 1024 * 1024 / 8;
		for (int i=0; i < numberOfElements; i++) {
			dataAccess2.setInt(i);
			sum1 += i;
			dataAccess2.setDouble(i);
			sum1 += i;
		}
		int sum2 = 0;
		dataAccess2.setCurrentPosition(0);
		for (long i=0; i < numberOfElements; i++) {
			sum2 += dataAccess2.getInt();
			sum2 += dataAccess2.getDouble();
		}
		assertEquals(sum1, sum2);
	}
		
}
