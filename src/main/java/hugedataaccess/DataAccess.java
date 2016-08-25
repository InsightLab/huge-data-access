package hugedataaccess;

public interface DataAccess {

	public long getCapacity();

	public boolean isEmpty();

	public void ensureCapacity(long bytes);

	public byte getByte();
	
	public byte getByte(long bytePos);
	
	public void setByte(byte element);
	
	public void setByte(long bytePos, byte element);

	/**
	 * Relative get method for reading a char value.
	 * Reads the next two bytes at this buffer's current position, 
	 * composing them into a char value according to the current 
	 * byte order, and then increments the position by two.
	 * @return The char value at the current position
	 */
	public char getChar();
	
	/**
	 * Absolute get method for reading a char value.
	 * Reads two bytes at the given index, composing them into a 
	 * char value according to the current byte order.
	 * @param bytePos The index (byte position) from which the bytes will be read
	 * @return The char value at the given index (byte position)
	 */
	public char getChar(long bytePos);

	/**
	 * Relative put method for writing a char value  (optional operation).
	 * Writes two bytes containing the given char value, in the current byte 
	 * order, into this buffer at the current position, and then increments 
	 * the position by two.
	 * @param element The char value to be written
	 */
	public void setChar(char element);
	
	/**
	 * Absolute put method for writing a char value  (optional operation).
	 * Writes two bytes containing the given char value, in the current byte 
	 * order, into this buffer at the given index.
	 * @param bytePos The index at which the bytes will be written
	 * @param element The char value to be written
	 */
	public void setChar(long bytePos, char element);
	
	public short getShort();
	
	public short getShort(long bytePos);

	public void setShort(short element);
	
	public void setShort(long bytePos, short element);
	
	public int getInt();
	
	public int getInt(long bytePos);

	public void setInt(int element);
	
	public void setInt(long bytePos, int element);
	
	public long getLong();
	
	public long getLong(long bytePos);

	public void setLong(long element);
	
	public void setLong(long bytePos, long element);
	
	public float getFloat();
	
	public float getFloat(long bytePos);

	public void setFloat(float element);
	
	public void setFloat(long bytePos, float element);

	public double getDouble();
	
	public double getDouble(long bytePos);

	public void setDouble(double element);
	
	public void setDouble(long bytePos, double element);

	public void close();
	
	public long getCurrentPosition();
	
	public void setCurrentPosition(long currentPosition);
	
}
