package hugedataaccess;


import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import hugedataaccess.MMapDataAccess;
import hugedataaccess.util.FileUtils;

public class MMapDataAccessTest extends AbstractDataAccessTest {

	private static String fileName = "MMapDataAccessTest.mmap";
	private static String fileName2 = "MMapDataAccessTest2.mmap";

	@BeforeClass
	public static void setUpClass() {
		FileUtils.delete(fileName);
		FileUtils.delete(fileName2);
	}
	
	@Before
	public void setUp() {
		dataAccess1 = new MMapDataAccess(fileName, 16);
		dataAccess1.ensureCapacity(64);
		dataAccess2 = new MMapDataAccess(fileName2);
		dataAccess2.ensureCapacity(1024 * 1024 * 2);
	}

	@Test(expected = DataAccessException.class)
	public void testCapacityAndBufferSizeCombination() throws IOException{
		long bytePos = 1099511627776L * 2;
		int segmentSize = 1024;
		File newFile = File.createTempFile("temporaryFile", ".tmp");
		FileUtils.delete(newFile);
		try {
			String fileName = newFile.getAbsolutePath();
			new MMapDataAccess(fileName, bytePos, segmentSize);
		} catch (RuntimeException e) {
			if (newFile.exists())  {
				throw new IOException("file should not exist");
			}
			throw e;
		}
	}
	
	@After
	public void tearDown() {
		dataAccess1.close();
		FileUtils.delete(fileName);
		dataAccess2.close();
		FileUtils.delete(fileName2);
	}
	
}
