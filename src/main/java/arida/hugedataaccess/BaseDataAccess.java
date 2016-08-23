package arida.hugedataaccess;

public abstract class BaseDataAccess implements DataAccess {
	protected long currentPosition;
	protected int segmentSize;
	protected int segmentSizePower;
	protected int segmentSizeDivisor;
	protected static int defaultSegmentSize = 1024 * 1024;  // 1 MB

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