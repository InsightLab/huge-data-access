package hugedataaccess;

import hugedataaccess.util.ByteUtils;

public class ByteArrayDataAccess extends BaseDataAccess {

	private byte[][] segments;

	public ByteArrayDataAccess() {
		this(DEFAULT_SEGMENT_SIZE);
	}
	
	public ByteArrayDataAccess(int segmentSize) {
		setSegmentSize(segmentSize);
	}
	
	public long getCapacity() {
		return segments == null ? 0 : segments.length * segmentSize;
	}

	public void ensureCapacity(long bytes) {
		if ((bytes % segmentSize) != 0) {
			throw new DataAccessException("Capacity must be a multiple of segment size.");
		}
		int expectedNumberOfSegments = (int) Math.ceil(1d * bytes / segmentSize);
		int currentNumberOfSegments = segments == null ? 0 : segments.length;
		
		if (expectedNumberOfSegments > currentNumberOfSegments) {
			byte[][] newSegments = new byte[expectedNumberOfSegments][];
			if (segments != null) {
				for (int i = 0; i < currentNumberOfSegments; i++) {
					newSegments[i] = segments[i];
				}
			}
			for (int i = currentNumberOfSegments; i < expectedNumberOfSegments; i++) {
				newSegments[i] = new byte[segmentSize];
			}
			segments = newSegments;
		}
	}

	public byte getByte(long bytePos) {
		// We are using bit operations to improve time performance:
		// (int) bytePos >> segmentSizePower   == (int) (bytePos / segmentSize)
		// (int)(bytePos & segmentSizeDivisor) == (int) (bytePos % segmentSize)
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return segments[segmentIndex][segmentPos];	
	}
	
	public void setByte(long bytePos, byte element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		segments[segmentIndex][segmentPos] = element;
	}

	public char getChar(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return ByteUtils.getChar(segments[segmentIndex], segmentPos);
	}

	public void setChar(long bytePos, char element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		ByteUtils.setChar(segments[segmentIndex], element, segmentPos);
	}

	public short getShort(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return ByteUtils.getShort(segments[segmentIndex], segmentPos);
	}

	public void setShort(long bytePos, short element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		ByteUtils.setShort(segments[segmentIndex], element, segmentPos);
	}

	public int getInt(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return ByteUtils.getInt(segments[segmentIndex], segmentPos);
	}

	public void setInt(long bytePos, int element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		ByteUtils.setInt(segments[segmentIndex], element, segmentPos);
	}

	public long getLong(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return ByteUtils.getLong(segments[segmentIndex], segmentPos);
	}

	public void setLong(long bytePos, long element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		ByteUtils.setLong(segments[segmentIndex], element, segmentPos);
	}

	public float getFloat(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return ByteUtils.getFloat(segments[segmentIndex], segmentPos);
	}

	public void setFloat(long bytePos, float element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		ByteUtils.setFloat(segments[segmentIndex], element, segmentPos);
	}

	public double getDouble(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return ByteUtils.getDouble(segments[segmentIndex], segmentPos);
	}

	public void setDouble(long bytePos, double element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		ByteUtils.setDouble(segments[segmentIndex], element, segmentPos);
	}
	
	public void close() {
		segments = null;
	}

}
