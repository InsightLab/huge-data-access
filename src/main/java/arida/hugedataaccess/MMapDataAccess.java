package arida.hugedataaccess;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class MMapDataAccess implements DataAccess {
	
	private RandomAccessFile randomAccessFile;
	private List<ByteBuffer> buffers;
	private int bufferSize;
	private static int defaultBufferSize = 1024 * 1024;  // 1 MB
	
	public MMapDataAccess(String fileName) {
		this(fileName, defaultBufferSize);
	}
	
	public MMapDataAccess(String fileName, int bufferSize) {
		try {
			randomAccessFile = new RandomAccessFile(fileName, "rw");
			buffers = new ArrayList<ByteBuffer>();
			setBufferSize(bufferSize);
		} catch (IOException e) {
			throw new DataAccessException(e.getMessage(), e);
		}
	}

	public void ensureCapacity(long bytes) {
		try {
			if ((bytes % bufferSize) != 0) {
				throw new DataAccessException("Capacity must be a multiple of buffer size.");
			}
			if (getCapacity() > bytes) { //Current file size is greater than the number of bytes to be mapped
				bytes = getCapacity();
			}
			int expectedNumberOfBuffers = (int) Math.ceil(1d * bytes / bufferSize);
			int currentNumberOfBuffers = buffers.size();
			
			if (expectedNumberOfBuffers > currentNumberOfBuffers) {
				for (int i = currentNumberOfBuffers; i < expectedNumberOfBuffers; i++) {
					int startPos = i * bufferSize;
					//System.out.println("buffer#, startPos: " + i + ", " + startPos);
					ByteBuffer buffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, startPos, bufferSize);
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

	public byte getByte(long bytePos) {
		int bufferIndex = (int) (bytePos / bufferSize);
		int bufferPos =   (int) (bytePos % bufferSize);
		System.out.println("bytePos, bufferIndex, bufferPos: " + bytePos + ", " + bufferIndex + ", " + bufferPos);
		return buffers.get(bufferIndex).get(bufferPos);	
	}
	
	public void setByte(long bytePos, byte element) {
		int bufferIndex = (int) (bytePos / bufferSize);
		int bufferPos =   (int) (bytePos % bufferSize);
		System.out.println("bytePos, bufferIndex, bufferPos: " + bytePos + ", " + bufferIndex + ", " + bufferPos);
		buffers.get(bufferIndex).put(bufferPos, element);
	}

	public char getChar(long bytePos) {
		int bufferIndex = (int) (bytePos / bufferSize);
		int bufferPos =   (int) (bytePos % bufferSize);
		System.out.println("bytePos, bufferIndex, bufferPos: " + bytePos + ", " + bufferIndex + ", " + bufferPos);
		return buffers.get(bufferIndex).getChar(bufferPos);	
	}
	
	public void setChar(long bytePos, char element) {
		int bufferIndex = (int) (bytePos / bufferSize);
		int bufferPos =   (int) (bytePos % bufferSize);
		System.out.println("bytePos, bufferIndex, bufferPos: " + bytePos + ", " + bufferIndex + ", " + bufferPos);
		buffers.get(bufferIndex).putChar(bufferPos, element);
	}

	public short getShort(long bytePos) {
		int bufferIndex = (int) (bytePos / bufferSize);
		int bufferPos =   (int) (bytePos % bufferSize);
		System.out.println("bytePos, bufferIndex, bufferPos: " + bytePos + ", " + bufferIndex + ", " + bufferPos);
		return buffers.get(bufferIndex).getShort(bufferPos);	
	}
	
	public void setShort(long bytePos, short element) {
		int bufferIndex = (int) (bytePos / bufferSize);
		int bufferPos =   (int) (bytePos % bufferSize);
		System.out.println("bytePos, bufferIndex, bufferPos: " + bytePos + ", " + bufferIndex + ", " + bufferPos);
		buffers.get(bufferIndex).putShort(bufferPos, element);
	}

	public int getInt(long bytePos) {
		int bufferIndex = (int) (bytePos / bufferSize);
		int bufferPos =   (int) (bytePos % bufferSize);
		System.out.println("bytePos, bufferIndex, bufferPos: " + bytePos + ", " + bufferIndex + ", " + bufferPos);
		return buffers.get(bufferIndex).getInt(bufferPos);	
	}
	
	public void setInt(long bytePos, int element) {
		int bufferIndex = (int) (bytePos / bufferSize);
		int bufferPos =   (int) (bytePos % bufferSize);
		System.out.println("bytePos, bufferIndex, bufferPos: " + bytePos + ", " + bufferIndex + ", " + bufferPos);
		buffers.get(bufferIndex).putInt(bufferPos, element);
	}

	public long getLong(long bytePos) {
		int bufferIndex = (int) (bytePos / bufferSize);
		int bufferPos =   (int) (bytePos % bufferSize);
		System.out.println("bytePos, bufferIndex, bufferPos: " + bytePos + ", " + bufferIndex + ", " + bufferPos);
		return buffers.get(bufferIndex).getLong(bufferPos);	
	}
	
	public void setLong(long bytePos, long element) {
		int bufferIndex = (int) (bytePos / bufferSize);
		int bufferPos =   (int) (bytePos % bufferSize);
		System.out.println("bytePos, bufferIndex, bufferPos: " + bytePos + ", " + bufferIndex + ", " + bufferPos);
		buffers.get(bufferIndex).putLong(bufferPos, element);
	}

	public float getFloat(long bytePos) {
		int bufferIndex = (int) (bytePos / bufferSize);
		int bufferPos =   (int) (bytePos % bufferSize);
		System.out.println("bytePos, bufferIndex, bufferPos: " + bytePos + ", " + bufferIndex + ", " + bufferPos);
		return buffers.get(bufferIndex).getFloat(bufferPos);	
	}

	public void setFloat(long bytePos, float element) {
		int bufferIndex = (int) (bytePos / bufferSize);
		int bufferPos =   (int) (bytePos % bufferSize);
		System.out.println("bytePos, bufferIndex, bufferPos: " + bytePos + ", " + bufferIndex + ", " + bufferPos);
		buffers.get(bufferIndex).putFloat(bufferPos, element);
	}

	public double getDouble(long bytePos) {
		int bufferIndex = (int) (bytePos / bufferSize);
		int bufferPos =   (int) (bytePos % bufferSize);
		System.out.println("bytePos, bufferIndex, bufferPos: " + bytePos + ", " + bufferIndex + ", " + bufferPos);
		return buffers.get(bufferIndex).getDouble(bufferPos);	
	}

	public void setDouble(long bytePos, double element) {
		int bufferIndex = (int) (bytePos / bufferSize);
		int bufferPos =   (int) (bytePos % bufferSize);
		System.out.println("bytePos, bufferIndex, bufferPos: " + bytePos + ", " + bufferIndex + ", " + bufferPos);
		buffers.get(bufferIndex).putDouble(bufferPos, element);
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

	public int getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
		//this.bufferSizePower = (int) (Math.log(bufferSize) / Math.log(2));
		//this.bufferSizeDivisor = bufferSize - 1;
	}
	
}
