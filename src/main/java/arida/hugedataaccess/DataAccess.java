package arida.hugedataaccess;

public interface DataAccess {

	public long size();

	public boolean isEmpty();

	public void ensureCapacity(long bytes);


	public void addDouble(double element);

	public void setDouble(long bytePos, double element);

	public double getDouble(long bytePos);

	
	public void close();
	
}
