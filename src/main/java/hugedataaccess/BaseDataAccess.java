package hugedataaccess;

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
		int segmentRemainder = this.segmentSize - (int) this.currentPosition & segmentSizeDivisor;
		
		if(segmentRemainder < 2){
			//the next char don't fit in this segment, go to next segment
			this.currentPosition = this.currentPosition + segmentRemainder;
		}
		
		return getChar((currentPosition += 2) - 2);  // post increment of more than 1
	}

	public void setChar(char element) {
		int segmentRemainder = this.segmentSize - (int) this.currentPosition & segmentSizeDivisor;
		
		if(segmentRemainder < 2){
			//the next char don't fit in this segment, go to next segment
			this.currentPosition = this.currentPosition + segmentRemainder;
		}
		
		setChar(currentPosition, element);
		currentPosition += 2;
	}
	
	public short getShort() {
		int segmentRemainder = this.segmentSize - (int) this.currentPosition & segmentSizeDivisor;
		
		if(segmentRemainder < 2){
			//the next short don't fit in this segment, go to next segment
			this.currentPosition = this.currentPosition + segmentRemainder;
		}
		
		return getShort((currentPosition += 2) - 2);
	}

	public void setShort(short element) {
		int segmentRemainder = this.segmentSize - (int) this.currentPosition & segmentSizeDivisor;
		
		if(segmentRemainder < 2){
			//the next short don't fit in this segment, go to next segment
			this.currentPosition = this.currentPosition + segmentRemainder;
		}
		
		setShort(currentPosition, element);
		currentPosition += 2;
	}
	
	public int getInt() {
		int segmentRemainder = this.segmentSize - (int) this.currentPosition & segmentSizeDivisor;
		
		if(segmentRemainder < 4){
			//the next integer don't fit in this segment, go to next segment
			this.currentPosition = this.currentPosition + segmentRemainder;
		}
		
		return getInt((currentPosition += 4) - 4);
	}

	public void setInt(int element) {
		int segmentRemainder = this.segmentSize - (int) this.currentPosition & segmentSizeDivisor;
		
		if(segmentRemainder < 4){
			//the next integer don't fit in this segment, go to next segment
			this.currentPosition = this.currentPosition + segmentRemainder;
		}
		
		setInt(currentPosition, element);
		currentPosition += 4;
	}
	
	public long getLong() {
		int segmentRemainder = this.segmentSize - (int) this.currentPosition & segmentSizeDivisor;
		
		if(segmentRemainder < 8){
			//the next long don't fit in this segment, go to next segment
			this.currentPosition = this.currentPosition + segmentRemainder;
		}
		
		return getLong((currentPosition += 8) - 8);
	}

	public void setLong(long element) {
		int segmentRemainder = this.segmentSize - (int) this.currentPosition & segmentSizeDivisor;
		
		if(segmentRemainder < 8){
			//the next long don't fit in this segment, go to next segment
			this.currentPosition = this.currentPosition + segmentRemainder;
		}
		
		setLong(currentPosition, element);
		currentPosition += 8;
	}
	
	public float getFloat() {
		int segmentRemainder = this.segmentSize - (int) this.currentPosition & segmentSizeDivisor;
		
		if(segmentRemainder < 4){
			//the next float don't fit in this segment, go to next segment
			this.currentPosition = this.currentPosition + segmentRemainder;
		}
		
		return getFloat((currentPosition += 4) - 4);
	}

	public void setFloat(float element) {
		int segmentRemainder = this.segmentSize - (int) this.currentPosition & segmentSizeDivisor;
		
		if(segmentRemainder < 4){
			//the next float don't fit in this segment, go to next segment
			this.currentPosition = this.currentPosition + segmentRemainder;
		}
		
		setFloat(currentPosition, element);
		currentPosition += 4;
	}
	
	public double getDouble() {
		int segmentRemainder = this.segmentSize - (int) this.currentPosition & segmentSizeDivisor;
		
		if(segmentRemainder < 4){
			//the next double don't fit in this segment, go to next segment
			this.currentPosition = this.currentPosition + segmentRemainder;
		}
		
		return getDouble((currentPosition += 8) - 8);
	}

	public void setDouble(double element) {
		int segmentRemainder = this.segmentSize - (int) this.currentPosition & segmentSizeDivisor;
		
		if(segmentRemainder < 4){
			//the next double don't fit in this segment, go to next segment
			this.currentPosition = this.currentPosition + segmentRemainder;
		}
		
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