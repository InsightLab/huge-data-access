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
	
	public MMapDataAccess(String fileName) {
		this(fileName, 1024 * 1024);
	}
	
	public MMapDataAccess(String fileName, int bufferSize) {
		if (bufferSize < 8) {
			throw new DataAccessException("Buffer size too short. Try to increase it.");
		}
		try {
			randomAccessFile = new RandomAccessFile(fileName, "rw");
			buffers = new ArrayList<ByteBuffer>();
			this.bufferSize = bufferSize;
		} catch (IOException e) {
			throw new DataAccessException(e.getMessage(), e);
		}
	}

	public void ensureCapacity(long bytes) {
		try {
			if (size() > bytes) { //Current file size is greater than the number of bytes to be mapped
				bytes = size();
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
	
	public int getBufferIndex(long bytePos) {
		return (int) (bytePos / bufferSize);
	}

	public int getBufferRelativePos(long bytePos) {
		return (int)(bytePos % bufferSize);
	}

	
	public long size() {
		try {
			return randomAccessFile.length();
		} catch (IOException e) {
			throw new DataAccessException(e.getMessage(), e);
		}
	}

	public boolean isEmpty() {
		return (randomAccessFile == null || this.size() == 0);
	}

	public void addDouble(double element) {
		ByteBuffer lastBuffer = buffers.get(buffers.size()-1);
		lastBuffer.putDouble(element);
	}

	public void setDouble(long bytePos, double element) {
		int bufferIndex = getBufferIndex(bytePos);
		int bufferPos = getBufferRelativePos(bytePos);
		buffers.get(bufferIndex).putDouble(bufferPos, element);
	}

	public double getDouble(long bytePos) {
		int bufferIndex = getBufferIndex(bytePos);
		int bufferPos = getBufferRelativePos(bytePos);
		//System.out.println("bufferIndex: " + bufferIndex);
		//System.out.println("bufferPos: " + bufferPos);
		return buffers.get(bufferIndex).getDouble(bufferPos);	
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
