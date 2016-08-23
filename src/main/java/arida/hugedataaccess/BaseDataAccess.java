package arida.hugedataaccess;

public abstract class BaseDataAccess implements DataAccess {
	protected long currentPosition;
	protected int segmentSize;
	protected int segmentSizePower;
	protected int segmentSizeDivisor;
	public static final int DEFAULT_SEGMENT_SIZE = 1024 * 1024;  // 1 MB

	public boolean isEmpty() {
		return (this.getCapacity() == 0);
	}
	
	public byte getByte() {
		return getByte(currentPosition++);
	}

	public void setByte(byte element) {
		setByte(currentPosition++, element);
	}
	
	public char getChar() {
		return getChar((currentPosition += 2) - 2);  // post increment of more than 1
	}

	public void setChar(char element) {
		setChar(currentPosition, element);
		currentPosition += 2;
	}
	
	public short getShort() {
		return getShort((currentPosition += 2) - 2);
	}

	public void setShort(short element) {
		setShort(currentPosition, element);
		currentPosition += 2;
	}
	
	public int getInt() {
		return getInt((currentPosition += 4) - 4);
	}

	public void setInt(int element) {
		setInt(currentPosition, element);
		currentPosition += 4;
	}
	
	public long getLong() {
		return getLong((currentPosition += 8) - 8);
	}

	public void setLong(long element) {
		setLong(currentPosition, element);
		currentPosition += 8;
	}
	
	public float getFloat() {
		return getFloat((currentPosition += 4) - 4);
	}

	public void setFloat(float element) {
		setFloat(currentPosition, element);
		currentPosition += 4;
	}
	
	public double getDouble() {
		return getDouble((currentPosition += 8) - 8);
	}

	public void setDouble(double element) {
		setDouble(currentPosition, element);
		currentPosition += 8;
	}
	
	public long getCurrentPosition() {
		return this.currentPosition;
	}
	
	public void setCurrentPosition(long currentPosition) {
		this.currentPosition = currentPosition;
	}
	
	protected void setSegmentSize(int segmentSize) {
		this.segmentSize = segmentSize;
		this.segmentSizePower = (int) (Math.log(segmentSize) / Math.log(2));
		if (Math.pow(2, segmentSizePower) != segmentSize) {
			throw new DataAccessException("segmentSize must be a power of 2.");
		}
		this.segmentSizeDivisor = segmentSize - 1;
	}

}