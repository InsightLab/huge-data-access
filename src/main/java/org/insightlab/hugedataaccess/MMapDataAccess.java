package org.insightlab.hugedataaccess;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.insightlab.hugedataaccess.util.FileUtils;

public class MMapDataAccess extends ByteBufferDataAccess {
	
	private RandomAccessFile randomAccessFile;
	private boolean isNewFile;
	
	public MMapDataAccess(String fileName) {
		this(fileName, DEFAULT_SEGMENT_SIZE);
	}
	
	public MMapDataAccess(String fileName, int bufferSize) {
		File f = new File(fileName);
		isNewFile = ! f.exists();
		try {
			setSegmentSize(bufferSize); // this must be before file creation
			randomAccessFile = new RandomAccessFile(fileName, "rw");
		} catch (IOException e) {
			throw new DataAccessException(e.getMessage(), e);
		}
	}
	
	public MMapDataAccess(String fileName, long bytesCapacity) throws IOException {
		this(fileName, bytesCapacity, DEFAULT_SEGMENT_SIZE);
	}
	
	public MMapDataAccess(String fileName, long bytesCapacity, int bufferSize) throws IOException {
		this(fileName, bufferSize);
		try {
			ensureCapacity(bytesCapacity);
		} catch (DataAccessException e) {
			if (isNewFile) {
				randomAccessFile.close();
				FileUtils.delete(fileName);
			}
			throw e;
		}
	}

	private void validateCapacity(long numberOfBytes) {
		if ((numberOfBytes / segmentSize) >= Integer.MAX_VALUE) {
			throw new DataAccessException("Buffer size is too small to fit in the given capacity.");
		}
		if ((numberOfBytes % segmentSize) != 0) {
			throw new DataAccessException("Capacity must be a multiple of buffer size.");
		}
	}

	public void ensureCapacity(long bytes) {
		long numberOfBytes = bytes;
		long startPos = -1;
		int segment = -1;
		try {
			if (getCapacity() > numberOfBytes) { //Current file size is greater than the number of bytes to be mapped
				numberOfBytes = getCapacity();
			}
			validateCapacity(numberOfBytes);
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
					startPos = (long) segment * (long) segmentSize;
					newBuffers[segment] = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, startPos, segmentSize);
				}
				buffers = newBuffers;
			}
		} catch (Exception e) {
			StringBuilder msg = new StringBuilder();
			msg.append("\nnumberOfBytes (capacity): ").append(numberOfBytes);
			msg.append("\nbufferSize: ").append(segmentSize);
			msg.append("\nstartPos: ").append(startPos);
			msg.append("\nsegment#: ").append(segment);
			msg.append("\n").append(e.getMessage());
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
			
			System.gc();
			
			if (randomAccessFile != null) {
				randomAccessFile.close();
			}
		} catch (IOException e) {
			throw new DataAccessException(e.getMessage(), e);
		}
	}

}