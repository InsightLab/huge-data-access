package hugedataaccess.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import hugedataaccess.DataAccessException;


public class FileUtils {

    public static void delete(String fileName) {
    	File f = new File(fileName);
    	if (f.exists()) {
    		delete(f);
    	}
    }
    
    public static void delete(File file) {
    	try {
			Files.delete(Paths.get(file.toURI()));
		} catch (IOException e) {
			throw new DataAccessException(e);
		}
    }
	
}
