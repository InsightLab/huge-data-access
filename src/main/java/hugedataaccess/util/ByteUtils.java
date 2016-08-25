package hugedataaccess.util;

public class ByteUtils {

	public static char getChar(byte[] bytes, int offset) {
        return (char) ((bytes[offset] & 0xFF) << 8 
                | (bytes[offset + 1] & 0xFF));
    }
	
    public static void setChar(byte[] bytes, char value, int offset) {
        bytes[offset]   = (byte) (value >> 8);
        bytes[offset + 1] = (byte) (value);
    }
	
	public static short getShort(byte[] bytes, int offset) {
        return (short) ((bytes[offset] & 0xFF) << 8 
                | (bytes[offset + 1] & 0xFF));
    }
	
    public static void setShort(byte[] bytes, short value, int offset) {
        bytes[offset]   = (byte) (value >> 8);
        bytes[offset + 1] = (byte) (value);
    }    
	
    public static int getInt(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFF) << 24 
        		    | (bytes[offset + 1] & 0xFF) << 16
                | (bytes[offset + 2] & 0xFF) << 8 
                | (bytes[offset + 3] & 0xFF);
    }
	
    public static void setInt(byte[] bytes, int value, int offset) {
        bytes[offset]   = (byte) (value >> 24);
        bytes[offset + 1] = (byte) (value >> 16);
        bytes[offset + 2] = (byte) (value >> 8);
        bytes[offset + 3] = (byte) (value);
    }    
    
    public static long getLong(byte[] bytes, int offset) {
        return ((long) getInt(bytes, offset) << 32) 
        			| (getInt(bytes, offset + 4) & 0xFFFFFFFFL);
    }
	
    public static void setLong(byte[] bytes, long value, int offset) {
        bytes[offset]   = (byte) (value >> 56);
        bytes[offset + 1] = (byte) (value >> 48);
        bytes[offset + 2] = (byte) (value >> 40);
        bytes[offset + 3] = (byte) (value >> 32);
        bytes[offset + 4] = (byte) (value >> 24);
        bytes[offset + 5] = (byte) (value >> 16);
        bytes[offset + 6] = (byte) (value >> 8);
        bytes[offset + 7] = (byte) (value);
    }
    
    public static float getFloat(byte[] bytes, int offset) {
    		int intValue = getInt(bytes, offset);
        return Float.intBitsToFloat(intValue);
    }
	
    public static void setFloat(byte[] bytes, float value, int offset) {
    		int intValue = Float.floatToIntBits(value);
    		setInt(bytes, intValue, offset);
    }

    public static double getDouble(byte[] bytes, int offset) {
		long longValue = getLong(bytes, offset);
		return Double.longBitsToDouble(longValue);
    }

    public static void setDouble(byte[] bytes, double value, int offset) {
		long longValue = Double.doubleToLongBits(value);
		setLong(bytes, longValue, offset);
    }

}