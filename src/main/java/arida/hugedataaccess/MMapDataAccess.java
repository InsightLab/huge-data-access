package arida.hugedataaccess;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class MMapDataAccess extends BaseDataAccess {
	
	private RandomAccessFile randomAccessFile;
	private ByteBuffer[] buffers;
	
	public MMapDataAccess(String fileName) {
		this(fileName, defaultSegmentSize);
	}
	
	public MMapDataAccess(String fileName, int bufferSize) {
		try {
			randomAccessFile = new RandomAccessFile(fileName, "rw");
			setSegmentSize(bufferSize);
		} catch (IOException e) {
			throw new DataAccessException(e.getMessage(), e);
		}
	}

	public void ensureCapacity(long bytes) {
		try {
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
					int startPos = i * segmentSize;
					newBuffers[i] = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, startPos, segmentSize);
				}
				buffers = newBuffers;
			}
		} catch (IOException e) {
			throw new DataAccessException(e.getMessage(), e);
		}
	}
	
	public long getCapacity() {
		try {
			return randomAccessFile.length();
		} catch (IOException e) {
			throw new DataAccessException(e.getMessage(), e);
		}
	}

	public boolean isEmpty() {
		return (randomAccessFile == null || this.getCapacity() == 0);
	}

	public byte getByte() {
		// We are using bit operations to improve time performance:
		// (int) bytePos >> segmentSizePower   == (int) (bytePos / segmentSize)
		// (int)(bytePos & segmentSizeDivisor) == (int) (bytePos % segmentSize)
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		currentPosition++;
		return buffers[segmentIndex].get(segmentPos);	
	}
	
	public byte getByte(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers[segmentIndex].get(segmentPos);	
	}
	
	public void setByte(byte element) {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		buffers[segmentIndex].put(segmentPos, element);
		currentPosition++;
	}

	public void setByte(long bytePos, byte element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers[segmentIndex].put(segmentPos, element);
	}

	public char getChar() {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		currentPosition += 2;
		return buffers[segmentIndex].getChar(segmentPos);	
	}
	
	public char getChar(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers[segmentIndex].getChar(segmentPos);	
	}
	
	public void setChar(char element) {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		buffers[segmentIndex].putChar(segmentPos, element);
		currentPosition += 2;
	}

	public void setChar(long bytePos, char element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers[segmentIndex].putChar(segmentPos, element);
	}

	public short getShort() {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		currentPosition += 2;
		return buffers[segmentIndex].getShort(segmentPos);	
	}
	
	public short getShort(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers[segmentIndex].getShort(segmentPos);	
	}
	
	public void setShort(short element) {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		buffers[segmentIndex].putShort(segmentPos, element);
		currentPosition += 2;
	}

	public void setShort(long bytePos, short element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers[segmentIndex].putShort(segmentPos, element);
	}

	public int getInt() {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		currentPosition += 4;
		return buffers[segmentIndex].getInt(segmentPos);	
	}
	
	public int getInt(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers[segmentIndex].getInt(segmentPos);	
	}
	
	public void setInt(int element) {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		buffers[segmentIndex].putInt(segmentPos, element);
		currentPosition += 4;
	}

	public void setInt(long bytePos, int element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers[segmentIndex].putInt(segmentPos, element);
	}

	public long getLong() {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		currentPosition += 8;
		return buffers[segmentIndex].getLong(segmentPos);	
	}
	
	public long getLong(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers[segmentIndex].getLong(segmentPos);	
	}
	
	public void setLong(long element) {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		buffers[segmentIndex].putLong(segmentPos, element);
		currentPosition += 8;
	}

	public void setLong(long bytePos, long element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers[segmentIndex].putLong(segmentPos, element);
	}

	public float getFloat() {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		currentPosition += 4;
		return buffers[segmentIndex].getFloat(segmentPos);	
	}

	public float getFloat(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers[segmentIndex].getFloat(segmentPos);	
	}

	public void setFloat(float element) {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		buffers[segmentIndex].putFloat(segmentPos, element);
		currentPosition += 4;
	}

	public void setFloat(long bytePos, float element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers[segmentIndex].putFloat(segmentPos, element);
	}

	public double getDouble() {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		currentPosition += 8;
		return buffers[segmentIndex].getDouble(segmentPos);
	}

	public double getDouble(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers[segmentIndex].getDouble(segmentPos);	
	}

	public void setDouble(double element) {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		buffers[segmentIndex].putDouble(segmentPos, element);
		currentPosition += 8;
	}

	public void setDouble(long bytePos, double element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers[segmentIndex].putDouble(segmentPos, element);
	}

	public void close() {
		try {
			if (buffers != null) {
				for (ByteBuffer buffer : buffers) {
					buffer.clear();
				}
				buffers = null;
			}
			if (randomAccessFile != null) {
				randomAccessFile.close();
			}
		} catch (IOException e) {
			throw new DataAccessException(e.getMessage(), e);
		}
	}

}
