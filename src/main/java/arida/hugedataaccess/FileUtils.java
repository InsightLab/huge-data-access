package arida.hugedataaccess;

import java.io.File;

public class FileUtils {

    public static void delete(String fileName) {
    		File file = new File(fileName);
    		if (file.exists()) {
        		delete(file);
    		}
    }

	
    public static void delete(File file) {
        if (!file.delete()) {
            String message = "Unable to delete file: " + file;
            throw new DataAccessException(message);
        }
    }
	
}
