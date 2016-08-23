package arida.hugedataaccess;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class MMapDataAccess extends BaseDataAccess {
	
	private RandomAccessFile randomAccessFile;
	private List<ByteBuffer> buffers;
	
	public MMapDataAccess(String fileName) {
		this(fileName, defaultSegmentSize);
	}
	
	public MMapDataAccess(String fileName, int bufferSize) {
		try {
			randomAccessFile = new RandomAccessFile(fileName, "rw");
			buffers = new ArrayList<ByteBuffer>();
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
				throw new DataAccessException("Capacity must be a multiple of buffer size.");
			}
			int expectedNumberOfBuffers = (int) Math.ceil(1d * bytes / segmentSize);
			int currentNumberOfBuffers = buffers.size();
			
			if (expectedNumberOfBuffers > currentNumberOfBuffers) {
				for (int i = currentNumberOfBuffers; i < expectedNumberOfBuffers; i++) {
					int startPos = i * segmentSize;
					ByteBuffer buffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, startPos, segmentSize);
					buffers.add(buffer);
				}
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
		// (int) bytePos >> segmentSizePower   == (int) (bytePos / bufferSize)
		// (int)(bytePos & segmentSizeDivisor) == (int) (bytePos % bufferSize)
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		currentPosition++;
		return buffers.get(segmentIndex).get(segmentPos);	
	}
	
	public byte getByte(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers.get(segmentIndex).get(segmentPos);	
	}
	
	public void setByte(byte element) {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		buffers.get(segmentIndex).put(segmentPos, element);
		currentPosition++;
	}

	public void setByte(long bytePos, byte element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers.get(segmentIndex).put(segmentPos, element);
	}

	public char getChar() {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		currentPosition += 2;
		return buffers.get(segmentIndex).getChar(segmentPos);	
	}
	
	public char getChar(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers.get(segmentIndex).getChar(segmentPos);	
	}
	
	public void setChar(char element) {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		buffers.get(segmentIndex).putChar(segmentPos, element);
		currentPosition += 2;
	}

	public void setChar(long bytePos, char element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers.get(segmentIndex).putChar(segmentPos, element);
	}

	public short getShort() {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		currentPosition += 2;
		return buffers.get(segmentIndex).getShort(segmentPos);	
	}
	
	public short getShort(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers.get(segmentIndex).getShort(segmentPos);	
	}
	
	public void setShort(short element) {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		buffers.get(segmentIndex).putShort(segmentPos, element);
		currentPosition += 2;
	}

	public void setShort(long bytePos, short element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers.get(segmentIndex).putShort(segmentPos, element);
	}

	public int getInt() {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		currentPosition += 4;
		return buffers.get(segmentIndex).getInt(segmentPos);	
	}
	
	public int getInt(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers.get(segmentIndex).getInt(segmentPos);	
	}
	
	public void setInt(int element) {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		buffers.get(segmentIndex).putInt(segmentPos, element);
		currentPosition += 4;
	}

	public void setInt(long bytePos, int element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers.get(segmentIndex).putInt(segmentPos, element);
	}

	public long getLong() {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		currentPosition += 8;
		return buffers.get(segmentIndex).getLong(segmentPos);	
	}
	
	public long getLong(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers.get(segmentIndex).getLong(segmentPos);	
	}
	
	public void setLong(long element) {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		buffers.get(segmentIndex).putLong(segmentPos, element);
		currentPosition += 8;
	}

	public void setLong(long bytePos, long element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers.get(segmentIndex).putLong(segmentPos, element);
	}

	public float getFloat() {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		currentPosition += 4;
		return buffers.get(segmentIndex).getFloat(segmentPos);	
	}

	public float getFloat(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers.get(segmentIndex).getFloat(segmentPos);	
	}

	public void setFloat(float element) {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		buffers.get(segmentIndex).putFloat(segmentPos, element);
		currentPosition += 4;
	}

	public void setFloat(long bytePos, float element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers.get(segmentIndex).putFloat(segmentPos, element);
	}

	public double getDouble() {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		currentPosition += 8;
		return buffers.get(segmentIndex).getDouble(segmentPos);
	}

	public double getDouble(long bytePos) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		return buffers.get(segmentIndex).getDouble(segmentPos);	
	}

	public void setDouble(double element) {
		int segmentIndex = (int) currentPosition >> segmentSizePower;
		int segmentPos =   (int)(currentPosition & segmentSizeDivisor);
		buffers.get(segmentIndex).putDouble(segmentPos, element);
		currentPosition += 8;
	}

	public void setDouble(long bytePos, double element) {
		int segmentIndex = (int) bytePos >> segmentSizePower;
		int segmentPos =   (int)(bytePos & segmentSizeDivisor);
		buffers.get(segmentIndex).putDouble(segmentPos, element);
	}

	public void close() {
		try {
			if (buffers != null) {
				for (ByteBuffer buffer : buffers) {
					buffer.clear();
				}
				buffers.clear();
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
