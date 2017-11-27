package org.insightlab.hugedataaccess.util;

import static org.junit.Assert.*;

import org.insightlab.hugedataaccess.util.ByteUtils;
import org.junit.Test;


public class ByteUtilsTest {

	@Test
	public void setIntGetInt() {
		byte[] bytes = new byte[100];
		int offset = 10;
		
		int value = 5;
		ByteUtils.setInt(bytes, value, offset);
		assertEquals(value, ByteUtils.getInt(bytes, offset));
		
		value = -5;
		ByteUtils.setInt(bytes, value, offset);
		assertEquals(value, ByteUtils.getInt(bytes, offset));
		
		value = Integer.MAX_VALUE;
		ByteUtils.setInt(bytes, value, offset);
		assertEquals(value, ByteUtils.getInt(bytes, offset));
		
		value = Integer.MIN_VALUE;
		ByteUtils.setInt(bytes, value, offset);
		assertEquals(value, ByteUtils.getInt(bytes, offset));
	}
	
	@Test
	public void setShortGetShort() {
		byte[] bytes = new byte[100];
		int offset = 10;
		
		short value = 5;
		ByteUtils.setShort(bytes, value, offset);
		assertEquals(value, ByteUtils.getShort(bytes, offset));
		
		value = -5;
		ByteUtils.setShort(bytes, value, offset);
		assertEquals(value, ByteUtils.getShort(bytes, offset));
		
		value = Short.MAX_VALUE;
		ByteUtils.setShort(bytes, value, offset);
		assertEquals(value, ByteUtils.getShort(bytes, offset));
		
		value = Short.MIN_VALUE;
		ByteUtils.setShort(bytes, value, offset);
		assertEquals(value, ByteUtils.getShort(bytes, offset));
	}

	@Test
	public void setCharGetChar() {
		byte[] bytes = new byte[100];
		int offset = 10;
		
		char value = '5';
		ByteUtils.setChar(bytes, value, offset);
		assertEquals(value, ByteUtils.getChar(bytes, offset));
		
		value = 'a';
		ByteUtils.setChar(bytes, value, offset);
		assertEquals(value, ByteUtils.getChar(bytes, offset));
		
		value = Character.MAX_VALUE;
		ByteUtils.setChar(bytes, value, offset);
		assertEquals(value, ByteUtils.getChar(bytes, offset));
		
		value = Character.MIN_VALUE;
		ByteUtils.setChar(bytes, value, offset);
		assertEquals(value, ByteUtils.getChar(bytes, offset));
	}
	
	@Test
	public void setLongGetLong() {
		byte[] bytes = new byte[100];
		int offset = 10;
		
		long value = 5;
		ByteUtils.setLong(bytes, value, offset);
		assertEquals(value, ByteUtils.getLong(bytes, offset));
		
		value = -5;
		ByteUtils.setLong(bytes, value, offset);
		assertEquals(value, ByteUtils.getLong(bytes, offset));
		
		value = Long.MAX_VALUE;
		ByteUtils.setLong(bytes, value, offset);
		assertEquals(value, ByteUtils.getLong(bytes, offset));
		
		value = Long.MIN_VALUE;
		ByteUtils.setLong(bytes, value, offset);
		assertEquals(value, ByteUtils.getLong(bytes, offset));
	}
	
	@Test
	public void setFloatGetFloat() {
		byte[] bytes = new byte[100];
		int offset = 10;
		
		float value = 5.5f;
		ByteUtils.setFloat(bytes, value, offset);
		assertEquals(value, ByteUtils.getFloat(bytes, offset), 0.0001);
		
		value = -5.5f;
		ByteUtils.setFloat(bytes, value, offset);
		assertEquals(value, ByteUtils.getFloat(bytes, offset), 0.0001);
		
		value = Float.MAX_VALUE;
		ByteUtils.setFloat(bytes, value, offset);
		assertEquals(value, ByteUtils.getFloat(bytes, offset), 0.0001);
		
		value = Float.MIN_VALUE;
		ByteUtils.setFloat(bytes, value, offset);
		assertEquals(value, ByteUtils.getFloat(bytes, offset), 0.0001);
	}

	@Test
	public void setDoubleGetDouble() {
		byte[] bytes = new byte[100];
		int offset = 10;
		
		double value = 5.5;
		ByteUtils.setDouble(bytes, value, offset);
		assertEquals(value, ByteUtils.getDouble(bytes, offset), 0.0001);
		
		value = -5.5;
		ByteUtils.setDouble(bytes, value, offset);
		assertEquals(value, ByteUtils.getDouble(bytes, offset), 0.0001);
		
		value = Double.MAX_VALUE;
		ByteUtils.setDouble(bytes, value, offset);
		assertEquals(value, ByteUtils.getDouble(bytes, offset), 0.0001);
		
		value = Double.MIN_VALUE;
		ByteUtils.setDouble(bytes, value, offset);
		assertEquals(value, ByteUtils.getDouble(bytes, offset), 0.0001);
	}

}
