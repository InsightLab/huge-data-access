package arida.hugedataaccess;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class MMapDataAccess extends ByteBufferDataAccess {
	
	private RandomAccessFile randomAccessFile;
	
	public MMapDataAccess(String fileName) {
		this(fileName, DEFAULT_SEGMENT_SIZE);
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