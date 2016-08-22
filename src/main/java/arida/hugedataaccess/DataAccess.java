package arida.hugedataaccess;

public interface DataAccess {

	public long getCapacity();

	public boolean isEmpty();

	public void ensureCapacity(long bytes);

	public byte getByte(long bytePos);

	public void setByte(long bytePos, byte element);
	
	public char getChar(long bytePos);

	public void setChar(long bytePos, char element);
	
	public short getShort(long bytePos);

	public void setShort(long bytePos, short element);
	
	public int getInt(long bytePos);

	public void setInt(long bytePos, int element);
	
	public long getLong(long bytePos);

	public void setLong(long bytePos, long element);
	
	public float getFloat(long bytePos);

	public void setFloat(long bytePos, float element);

	public double getDouble(long bytePos);

	public void setDouble(long bytePos, double element);

	public void close();
	
}
