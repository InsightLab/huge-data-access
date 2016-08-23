package arida.hugedataaccess;

import java.nio.ByteBuffer;

public class ByteBufferDataAccess extends BaseDataAccess {
	
	protected ByteBuffer[] buffers;
	
	public ByteBufferDataAccess() {
		this(DEFAULT_SEGMENT_SIZE);
	}
	
	public ByteBufferDataAccess(int bufferSize) {
		setSegmentSize(bufferSize);
	}

	public void ensureCapacity(long bytes) {
		if (getCapacity() > bytes) { //Current file size is greater than the number of bytes to be mapped
			bytes = getCapacity();
		}
		if ((bytes % segmentSize) != 0) {
			throw new DataAccessException("Capacity must be a multiple of segment size.");
		}
		int expectedNumberOfBuffers = (int) Math.ceil(1d * bytes / segmentSize);
		int currentNumberOfBuffers = buffers == null ? 0 : buffers.length;
		
		if (expectedNumberOfBuffers > currentNumberOfBuffers) {
			ByteBuffer[] newBuffers = new ByteBuffer[expectedNumberOfBuffers];
			if (buffers != null) {
				for (int i = 0; i < currentNumberOfBuffers; i++) {
					newBuffers[i] = buffers[i];
				}
			}
			for (int i = currentNumberOfBuffers; i < expectedNumberOfBuffers; i++) {
				newBuffers[i] = ByteBuffer.allocate(segmentSize);
			}
			buffers = newBuffers;
		}
	}
	
	public long getCapacity() {
		return buffers == null ? 0 : buffers.length * segmentSize;
	}

	public byte getByte(long bytePos) {
		// We are using bit operations to improve time performance:
		// (int) bytePos >> segmentSizePower   == (int) (bytePos / segmentSize)
		// (int)(bytePos & segmentSizeDivisor) == (int) (bytePos % segmentSize)
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers[segmentIndex].get(segmentPos);	
	}
	
	public void setByte(long bytePos, byte element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers[segmentIndex].put(segmentPos, element);
	}

	public char getChar(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers[segmentIndex].getChar(segmentPos);	
	}
	
	public void setChar(long bytePos, char element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers[segmentIndex].putChar(segmentPos, element);
	}

	public short getShort(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers[segmentIndex].getShort(segmentPos);	
	}
	
	public void setShort(long bytePos, short element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers[segmentIndex].putShort(segmentPos, element);
	}

	public int getInt(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers[segmentIndex].getInt(segmentPos);	
	}
	
	public void setInt(long bytePos, int element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers[segmentIndex].putInt(segmentPos, element);
	}

	public long getLong(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers[segmentIndex].getLong(segmentPos);	
	}
	
	public void setLong(long bytePos, long element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers[segmentIndex].putLong(segmentPos, element);
	}

	public float getFloat(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers[segmentIndex].getFloat(segmentPos);	
	}

	public void setFloat(long bytePos, float element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers[segmentIndex].putFloat(segmentPos, element);
	}

	public double getDouble(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers[segmentIndex].getDouble(segmentPos);	
	}

	public void setDouble(long bytePos, double element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers[segmentIndex].putDouble(segmentPos, element);
	}

	public void close() {
		if (buffers != null) {
			for (ByteBuffer buffer : buffers) {
				buffer.clear();
			}
			buffers = null;
		}
	}

}
