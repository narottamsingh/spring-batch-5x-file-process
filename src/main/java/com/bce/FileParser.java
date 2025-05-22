package com.bce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class FileParser {

	public static void main(String[] args) {
		String filePath = "/home/narottam/gen.ctrl";
		try {
			FileParser parser=new FileParser();
			
			FileContent fileContent = parser.parseFile(filePath);
			System.out.println("Limit: " + fileContent.getLimit());
			System.out.println("Current Generation: " + fileContent.getCurrentGeneration());
			System.out.println("Old Generation: " + fileContent.getOldGeneration());
			String inputPath="/home/narottam/golang.0."+fileContent.getCurrentGeneration();
			String targetpath="/home/narottam/tmp/";
		   parser.copyFileToDirectory(inputPath, targetpath);
			
			//parser.updateVersion(fileContent);
		} catch (IOException e) {
			System.err.println("Error reading file: " + e.getMessage());
		}
	}

	public  FileContent parseFile(String filePath) throws IOException {
		FileContent fileContent = new FileContent();
		fileContent.setFilePath(filePath);
		try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
			String line;
			while ((line = reader.readLine()) != null) {
				if (line.startsWith("LIMIT")) {
					fileContent.setLimit(Integer.parseInt(line.split(" ")[1]));
				} else if (line.startsWith("CURRENT_GENERATION")) {
					fileContent.setCurrentGeneration(Integer.parseInt(line.split(" ")[1]));
				} else if (line.startsWith("OLDEST_GENERATION")) {
					fileContent.setOldGeneration(Integer.parseInt(line.split(" ")[1]));
				}
			}
		}
		return fileContent;
	}

	public  void updateVersion(FileContent fileGeneration) throws IOException {
		File inputFile = new File(fileGeneration.getFilePath());
		  File tempFile = new File(fileGeneration.getFilePath()+".tmp"); //
		try (BufferedReader reader = new BufferedReader(new FileReader(inputFile));
				BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
			String line;
			while ((line = reader.readLine()) != null) {
				
				if (line.startsWith("CURRENT_GENERATION")) {
					// Replace the old version with the new version
					line = "CURRENT_GENERATION " + (fileGeneration.currentGeneration+1);
				}
				if (line.startsWith("OLDEST_GENERATION")) {
					// Replace the old version with the new version
					line = "OLDEST_GENERATION " + (fileGeneration.oldGeneration+1);
				}
				writer.write(line + System.lineSeparator());
			}
		}
		// Replace the original file with the temporary file
        if (inputFile.delete()) {
            tempFile.renameTo(inputFile);
        } else {
            throw new IOException("Error updating version: Failed to replace the original file.");
        }

	}
	
	 public void copyFileToDirectory(String sourceFilePath, String targetDirectoryPath) throws IOException {
	        Path sourceFile = Paths.get(sourceFilePath);
	        Path targetDirectory = Paths.get(targetDirectoryPath);

	        // Get the filename from the source file path
	        String fileName = sourceFile.getFileName().toString();

	        // Construct the target file path
	        Path targetFile = targetDirectory.resolve(fileName);

	        // Copy the source file to the target directory
	        Files.copy(sourceFile, targetFile,StandardCopyOption.REPLACE_EXISTING);
	    }

class FileContent {
    private int limit;
    private int currentGeneration;
    private int oldGeneration;
    private String filePath;
    // Getter and setter methods for limit, currentGeneration, and oldGeneration

    public String getFilePath() {
	return filePath;
	}
	
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public int getCurrentGeneration() {
        return currentGeneration;
    }

    public void setCurrentGeneration(int currentGeneration) {
        this.currentGeneration = currentGeneration;
    }

    public int getOldGeneration() {
        return oldGeneration;
    }

    public void setOldGeneration(int oldGeneration) {
        this.oldGeneration = oldGeneration;
    }
}
}
