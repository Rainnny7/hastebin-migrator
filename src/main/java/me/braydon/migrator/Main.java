package me.braydon.migrator;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.NonNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

/**
 * @author Braydon
 */
public class Main {
    public static void main(@NonNull String[] args) {
        String redisUri = System.getenv("REDIS_URI"); // Get the redis uri env var
        assert redisUri != null; // We need this
        
        File dataDir = new File("data"); // The directory where the files are located
        if (!dataDir.exists()) {
            dataDir.mkdirs();
            System.out.println("Didn't find the './data' directory, created it for you (place files here)");
            return;
        }
        File[] files = dataDir.listFiles();
        if (files == null || (files.length == 0)) { // No files to iterate
            System.out.println("No files to copy");
            return;
        }
        try (RedisClient redisClient = RedisClient.create(redisUri);
             StatefulRedisConnection<String, String> connection = redisClient.connect()
        ) {
            RedisAsyncCommands<String, String> asyncCommands = connection.async();
            asyncCommands.multi(); // Start the transaction
            int copied = 0; // The amount of files copied
            for (File file : files) {
                if (file.isDirectory()) { // Only care about files
                    continue;
                }
                String md5Hash = file.getName(); // The name is the md5 hash of the paste
                try {
                    List<String> fileLines = Files.readAllLines(file.toPath()); // The lines of the line
                    if (fileLines.isEmpty()) { // Skip empty files
                        continue;
                    }
                    StringBuilder stringBuilder = new StringBuilder();
                    for (String fileLine : fileLines) {
                        stringBuilder.append(fileLine).append("\n"); // Append the line to the string builder
                    }
                    String fileContent = stringBuilder.toString();
                    System.out.println("Copying '" + md5Hash + "' to Redis"); // Log the copy
                    asyncCommands.set(md5Hash, fileContent); // Copy the file to Redis
                    copied++; // Increment the amount of files copied
                    System.out.println("Done!");
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
            asyncCommands.exec(); // Execute the transaction
            System.out.println("Copied " + copied + "/" + files.length + " files to Redis"); // Log the copied files
        }
    }
}