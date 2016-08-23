package arida.hugedataaccess;


import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

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

	@After
	public void tearDown() {
		dataAccess1.close();
		FileUtils.delete(fileName);
		dataAccess2.close();
		FileUtils.delete(fileName2);
	}
	
}
