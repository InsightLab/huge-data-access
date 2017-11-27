package org.insightlab.hugedataaccess;

import static org.junit.Assert.assertEquals;

import org.insightlab.hugedataaccess.ByteBufferDataAccess;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ByteBufferDataAccessTest extends AbstractDataAccessTest {

	@Before
	public void setUp() {
		dataAccess1 = new ByteBufferDataAccess(16);
		dataAccess1.ensureCapacity(64);
		dataAccess2 = new ByteBufferDataAccess();
		dataAccess2.ensureCapacity(1024 * 1024 * 2);
	}
	
	@Test
	public void testCaseManageSet1(){
		
		int currentPositionCorrect = 24;
		int currentPositionResult;
		
		//Test if after insert int, the next insert go to next segment.
		dataAccess1.setDouble(10);
		dataAccess1.setInt(4);
		dataAccess1.setLong(12);
		
		currentPositionResult = (int) dataAccess1.getCurrentPosition();
		
		assertEquals(currentPositionCorrect,currentPositionResult);
		
		currentPositionCorrect = 40;
		dataAccess1.setFloat(2);
		dataAccess1.setLong(15);
		
		currentPositionResult = (int) dataAccess1.getCurrentPosition();
		assertEquals(currentPositionCorrect,currentPositionResult);
		

	}
	
	@Test
	public void testCaseManageSet2(){
		
		int currentPositionCorrect = 24;
		int currentPositionResult;
		
		//Test if after insert char, the next insert go to next segment.
		dataAccess1.setDouble(10);
		dataAccess1.setChar('c');
		dataAccess1.setLong(12);
		
		currentPositionResult = (int) dataAccess1.getCurrentPosition();
		
		assertEquals(currentPositionCorrect,currentPositionResult);
		
		currentPositionCorrect = 36;
		dataAccess1.setChar('a');
		dataAccess1.setChar('b');
		dataAccess1.setChar('c');
		dataAccess1.setInt(8);
		
		
		currentPositionResult = (int) dataAccess1.getCurrentPosition();
		assertEquals(currentPositionCorrect,currentPositionResult);

	}
	
	@Test
	public void testCaseManageGet(){
		
		int currentPositionCorrect = 24;
		int currentPositionResult;
		double valueDouble;
		int valueInt;
		long valueLong;
		
		//Insert data
		dataAccess1.setDouble(10);
		dataAccess1.setInt(4);
		dataAccess1.setLong(12);
		
		//Retrieve data
		dataAccess1.setCurrentPosition(0);
		valueDouble = dataAccess1.getDouble();
		valueInt = dataAccess1.getInt();
		valueLong = dataAccess1.getLong();
		
		currentPositionResult = (int) dataAccess1.getCurrentPosition();
		
		assertEquals(10, valueDouble, 0);
		assertEquals(4, valueInt, 0);
		assertEquals(12, valueLong, 0);
		assertEquals(currentPositionCorrect,currentPositionResult);
	}
	
	@Test
	public void testCaseManageGet2(){
		
		int currentPositionCorrect = 24;
		int currentPositionResult;
		double valueDouble;
		float valueFloat;
		long valueLong;
		
		//Insert data
		dataAccess1.setDouble(10);
		dataAccess1.setFloat(4);
		dataAccess1.setLong(12);
		
		//Retrieve data
		dataAccess1.setCurrentPosition(0);
		valueDouble = dataAccess1.getDouble();
		valueFloat = dataAccess1.getFloat();
		valueLong = dataAccess1.getLong();
		
		currentPositionResult = (int) dataAccess1.getCurrentPosition();
		
		assertEquals(10, valueDouble, 0);
		assertEquals(4, valueFloat, 0);
		assertEquals(12, valueLong, 0);
		assertEquals(currentPositionCorrect,currentPositionResult);
	}
	
	@Test
	public void testCaseManageGet3(){
		
		int currentPositionCorrect = 24;
		int currentPositionResult;
		double valueDouble;
		char valueChar;
		long valueLong;
		
		//Insert data
		dataAccess1.setDouble(10);
		dataAccess1.setChar('c');
		dataAccess1.setLong(12);
		
		//Retrieve data
		dataAccess1.setCurrentPosition(0);
		valueDouble = dataAccess1.getDouble();
		valueChar = dataAccess1.getChar();
		valueLong = dataAccess1.getLong();
		
		currentPositionResult = (int) dataAccess1.getCurrentPosition();
		
		assertEquals(10, valueDouble, 0);
		assertEquals('c', valueChar, 0);
		assertEquals(12, valueLong, 0);
		assertEquals(currentPositionCorrect,currentPositionResult);
	}
	
	@After
	public void tearDown() {
		dataAccess1.close();
		dataAccess2.close();
	}
	
}
