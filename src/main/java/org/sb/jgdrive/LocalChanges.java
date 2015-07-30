package org.sb.jgdrive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.xml.bind.DatatypeConverter;

import com.google.api.services.drive.model.File;

public class LocalChanges
{
    private static final Logger log = Logger.getLogger(Pull.class.getPackage().getName());
    private final Map<Path, String> modifiedFiles;
    private final Set<Path> newFiles;
    private final Map<Path, String> deletedPaths;
    private final Map<Entry<Path, String>, Path> movedFilesFromTo;
    private final boolean empty;
    
    public LocalChanges(final Driver driver) throws IOException
    {
        this(driver, Optional.empty());
    }
    
    public LocalChanges(final Driver driver, Optional<FileTime> fromTime) throws IOException
    {
        Path home = driver.getHome();
        RemoteIndex ri = driver.getRemoteIndex();
        deletedPaths = ri.localPaths().filter(e -> Files.notExists(home.resolve(e.getKey()), LinkOption.NOFOLLOW_LINKS))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        log.fine(() -> "Found deleted " + deletedPaths.keySet()); 
        Set<Path> allChangedFiles = driver.getLocalModifiedFiles(fromTime).collect(Collectors.toSet());
        if(allChangedFiles.size() > 0)
        {
            modifiedFiles = ri.getFileId(allChangedFiles.stream());
            newFiles = allChangedFiles.stream().filter(p -> !modifiedFiles.containsKey(p)).collect(Collectors.toSet());
            modifiedFiles.keySet().removeAll(deletedPaths.keySet()); //those files that were previously recorded as modified
            movedFilesFromTo = detectMovedFiles(driver, home, newFiles, deletedPaths);
            newFiles.removeAll(movedFilesFromTo.values());
            deletedPaths.keySet().removeAll(movedFilesFromTo.keySet().stream().map(p -> p.getKey()).collect(Collectors.toSet()));
        }
        else
        {
            modifiedFiles = Collections.emptyMap();
            newFiles = Collections.emptySet();
            movedFilesFromTo = Collections.emptyMap();
        }
        empty = allChangedFiles.isEmpty() && deletedPaths.isEmpty();
    }

    public boolean isEmpty()
    {
        return empty;
    }
    
    public Map<Path, String> getModifiedFiles()
    {
        return modifiedFiles;
    }

    public Set<Path> getNewFiles()
    {
        return newFiles;
    }

    public Map<Path, String> getDeletedPaths()
    {
        return deletedPaths;
    }

    public Map<Entry<Path, String>, Path> getMovedFilesFromTo()
    {
        return movedFilesFromTo;
    }

    private static Map<Entry<Path, String>, Path> detectMovedFiles(Driver driver, Path home, Set<Path> newFiles, Map<Path, String> deletedPaths) throws IOException
    {
        Map<String, File> idFileMap = driver.getFiles(deletedPaths.values().stream());
        final Function<Path, String> md5HashFunc = new Function<Path, String>()
        {
            private final ConcurrentHashMap<Path, byte[]> pathMd5Map = new ConcurrentHashMap<>();
            @Override
            public String apply(Path path)
            {
                byte[] hash = pathMd5Map.get(path);
                if(hash == null) pathMd5Map.put(path, hash = md5Hash(path));
                return DatatypeConverter.printHexBinary(hash).toLowerCase();
            }
            
            private byte[] md5Hash(Path path)
            {
                try
                {
                    MessageDigest md5 = MessageDigest.getInstance("MD5");
                    ByteChannel bc = Files.newByteChannel(path, StandardOpenOption.READ);
                    ByteBuffer bb = ByteBuffer.allocate(2048);
                    while(bc.read(bb) != -1)
                    {
                        bb.flip();
                        md5.update(bb);
                        bb.clear();
                    }
                    bc.close();
                    return md5.digest();
                }
                catch (NoSuchAlgorithmException | IOException e)
                {
                    throw new RuntimeException("Failed to generate MD5 hash of " + path, e);
                }
            }
        };
        
        return   
            newFiles.stream().parallel().map(path -> 
                deletedPaths.entrySet().stream().filter(e -> !Files.isDirectory(home.resolve(e.getKey())) &&
                                                            md5HashFunc.apply(home.resolve(path))
                                                                .equals(idFileMap.get(e.getValue()).getMd5Checksum()))
                                 .findFirst().<Entry<Entry<Path, String>, Path>>map(
                                         e -> new SimpleImmutableEntry<Entry<Path, String>, Path>(
                                                     new SimpleImmutableEntry<Path, String>(e.getKey(), e.getValue()), path)))
                         .filter(sie -> sie.isPresent()).map(sie -> sie.get())
                                .collect(Collectors.toMap(sie -> sie.getKey(), sie -> sie.getValue()));
    }

}
