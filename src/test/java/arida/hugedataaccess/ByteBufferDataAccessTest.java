package arida.hugedataaccess;

import org.junit.After;
import org.junit.Before;

public class ByteBufferDataAccessTest extends AbstractDataAccessTest {

	@Before
	public void setUp() {
		dataAccess1 = new ByteBufferDataAccess(16);
		dataAccess1.ensureCapacity(64);
		dataAccess2 = new ByteBufferDataAccess();
		dataAccess2.ensureCapacity(1024 * 1024 * 2);
	}
		
	@After
	public void tearDown() {
		dataAccess1.close();
		dataAccess2.close();
	}
	
}
