package hugedataaccess;

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
	
	public MMapDataAccess(String fileName, long bytesCapacity, int bufferSize) {
		try {
			randomAccessFile = new RandomAccessFile(fileName, "rw");
			setSegmentSize(bufferSize);
		} catch (IOException e) {
			throw new DataAccessException(e.getMessage(), e);
		}
		ensureCapacity(bytesCapacity);
	}


	public void ensureCapacity(long bytes) {
		long numberOfBytes = bytes;
		long startPos = -1;
		int segment = -1;
		try {
			if (getCapacity() > numberOfBytes) { //Current file size is greater than the number of bytes to be mapped
				numberOfBytes = getCapacity();
			}
			if ((numberOfBytes % segmentSize) != 0) {
				throw new DataAccessException("Capacity must be a multiple of segment size.");
			}
			int expectedNumberOfBuffers = (int) Math.ceil(1d * numberOfBytes / segmentSize);
			int currentNumberOfBuffers = buffers == null ? 0 : buffers.length;
			
			if (expectedNumberOfBuffers > currentNumberOfBuffers) {
				ByteBuffer[] newBuffers = new ByteBuffer[expectedNumberOfBuffers];
				if (buffers != null) {
					for (int i = 0; i < currentNumberOfBuffers; i++) {
						newBuffers[i] = buffers[i];
					}
				}
				for (segment = currentNumberOfBuffers; segment < expectedNumberOfBuffers; segment++) {
					startPos = segment * segmentSize;
					newBuffers[segment] = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, startPos, segmentSize);
				}
				buffers = newBuffers;
			}
		} catch (Exception e) {
			StringBuilder msg = new StringBuilder();
			msg.append("\nnumberOfBytes (capacity): ").append(numberOfBytes);
			msg.append("\nsegmentSize: ").append(segmentSize);
			msg.append("\nstartPos: ").append(startPos);
			msg.append("\nsegment#: ").append(segment);
			msg.append("\n");
			msg.append(e.getMessage());
			throw new DataAccessException(msg.toString(), e);
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