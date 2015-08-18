package org.sb.jgdrive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.xml.bind.DatatypeConverter;

import com.google.api.services.drive.model.File;

public class Pull implements Cmd
{
    private static final Logger log = Logger.getLogger(Pull.class.getPackage().getName());

    public void exec(final Driver driver, final List<String> opts) throws IOException, IllegalStateException
    {
        RemoteIndex ri = driver.getRemoteIndex();
        RemoteChanges remChanges = driver.getRemoteChanges(ri.getLastRevisionId(), Optional.of(ri.getLastSyncTime()));
        if(remChanges.getLargestChangeId() == ri.getLastRevisionId())
        {
            log.info("Already up to date at revision: " + ri.getLastRevisionId() + ". Nothing to pull.");
            return;
        }
        log.info("Pulling to revision: " + remChanges.getLargestChangeId());
        Path home = driver.getHome();
        LocalChanges lc = new LocalChanges(driver);
        ri.setLastSyncTime();
        
        Set<String> changedIds = remChanges.getChanges().stream().map(ch -> ch.getFile().getId()).collect(Collectors.toSet());
        
        Set<Path> conflicts = Stream.of(lc.getModifiedFiles().entrySet(), 
                                                    lc.getDeletedPaths().entrySet(), lc.getMovedFilesFromTo().keySet())
                               .flatMap(es -> es.stream())
                               .filter(e -> changedIds.contains(e.getValue()))
                               .map(e -> e.getKey()).collect(Collectors.toSet());
        
        
        /*Set<Path> conflicts = remChanges.getChanges().stream().map(ch -> ri.getLocalPath(ch.getFile().getId()))
                .filter(op -> op.map(p -> localChanges.contains(p)).orElse(false)).map(op -> op.get())*/
                
        if (!conflicts.isEmpty())
            throw new IllegalStateException("Found following local changes that conflict with remote changes. "
                    + "Please delete or move these local files: " + conflicts);
        
        List<Path> deletePaths = ri.remove(Stream.concat(remChanges.getDeletedFiles(), remChanges.getDeletedDirs()).map(f -> f.getId()));
        
        Map<File, Path> newFilePathMap = ri.add(Stream.concat(remChanges.getModifiedDirs(), remChanges.getModifiedFiles()));
        if(!Collections.disjoint(newFilePathMap.values(), lc.getNewFiles()))
            throw new IllegalStateException("Some new files conflict with upstream changes" + conflicts);
            
        Stream<Entry<File, Path>> remModifiedFiles = driver.downloadFiles(remChanges.getModifiedFiles()
                                                    .filter(f -> isModified(f.getMd5Checksum(), home.resolve(newFilePathMap.get(f)))).parallel());
        
        log.fine("Updating directories ...");
        remChanges.getModifiedDirs().forEach(f -> Optional.ofNullable(newFilePathMap.get(f)).map(Try.uncheckFunction(p -> createDir(home.resolve(p)))));
        
        log.fine("Updating files ...");
        remModifiedFiles.forEach(Try.uncheck(e -> moveFile(e.getValue(), 
                    home.resolve(Optional.ofNullable(newFilePathMap.get(e.getKey())).orElseThrow(
                            () -> new IllegalStateException("Remote file " + e.getKey() + " was not added to index."))))));

        log.fine("Deleting files ...");
        deletePaths.stream().forEach(Try.uncheck(p -> deletePath(home.resolve(p))));
        
        ri.setLastRevisionId(remChanges.getLargestChangeId());
        log.info("Updated to revision: " + ri.getLastRevisionId());
        driver.saveLocalChanges(lc.getModifiedFiles().keySet());
        driver.saveRemoteIndex();
    }
    
    private boolean isModified(String checkSum, Path path)
    {
        try
        {
            if(Files.notExists(path)) return true;
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
            return !checkSum.equalsIgnoreCase(DatatypeConverter.printHexBinary(md5.digest()));
        }
        catch (NoSuchAlgorithmException | IOException e)
        {
            throw new RuntimeException("Failed to generate MD5 hash of " + path, e);
        }
    }

    private boolean deletePath(Path p) throws IOException
    {
        log.info("Deleting local '" + p + "'");
        return Files.deleteIfExists(p);
    }

    private void moveFile(Path tmpPath, Path lp) throws IOException
    {
        log.info("Updating local '" + lp + "'");
        Files.move(tmpPath, lp, StandardCopyOption.REPLACE_EXISTING);
    }

    private boolean createDir(Path p) throws IOException
    {
        if(!Files.exists(p, LinkOption.NOFOLLOW_LINKS))
        {
            log.info("Adding local '" + p + "'");
            Files.createDirectories(p);
            return true;
        }
        return false;
    }
    
}
