package org.sb.jgdrive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        log.info("Updating to revision: " + remChanges.getLargestChangeId());
        Path home = driver.getHome();
        Set<Path> localChanges = driver.getLocalModifiedFiles().collect(Collectors.toSet());
        if(!localChanges.isEmpty())
        {
            Set<Path> conflicts = remChanges.getChanges().stream().map(ch -> ri.getLocalPath(ch.getFile().getId()))
                    .filter(op -> op.map(p -> localChanges.contains(p)).orElse(false)).map(op -> op.get())
                    .collect(Collectors.toSet());
            if (!conflicts.isEmpty())
            {
                throw new IllegalStateException("Found following local changes that conflict with remote changes. "
                        + "Please delete or move these local files: " + conflicts);
            }
        }
        
        Map<File, Path> remModifiedFiles = driver.downloadFiles(remChanges.getModifiedFiles().parallel());
        

        List<SimpleImmutableEntry<File, Optional<Path>>> deletePaths = 
                Stream.concat(remChanges.getDeletedFiles(), remChanges.getDeletedDirs())
                        .map(f -> new SimpleImmutableEntry<>(f, ri.getLocalPath(f.getId()))).collect(Collectors.toList());
        ri.remove(deletePaths.stream().map(e -> e.getKey().getId()));
        
        ri.add(Stream.concat(remChanges.getModifiedDirs(), remModifiedFiles.keySet().stream()));
        log.fine("Updating directories ...");
        remChanges.getModifiedDirs().forEach(f -> ri.getLocalPath(f.getId()).ifPresent(p -> createDir(home.resolve(p))));
        
        log.fine("Updating files ...");
        remModifiedFiles.entrySet().stream().forEach(e -> 
                ri.getLocalPath(e.getKey().getId()).ifPresent(lp -> moveFile(e.getValue(), home.resolve(lp))));

        log.fine("Deleting files ...");
        deletePaths.stream().map(e -> e.getValue()).forEach(op -> op.map(p -> deletePath(home.resolve(p))));
        
        ri.setLastRevisionId(remChanges.getLargestChangeId());
        log.info("Updated to revision: " + ri.getLastRevisionId());
        driver.saveRemoteIndex();
    }
    
    private boolean deletePath(Path p)
    {
        try
        {
            log.info("Deleting local " + p);
            return Files.deleteIfExists(p);
        }
        catch (IOException e)
        {
            throw new IORtException(e);
        }
    }

    private void moveFile(Path tmpPath, Path lp)
    {
        try
        {
            log.info("Updating local file " + lp + " with " + tmpPath);
            Files.move(tmpPath, lp, StandardCopyOption.REPLACE_EXISTING);
        }
        catch (IOException e)
        {
            throw new IORtException(e);
        }
    }

    private void createDir(Path p)
    {
        try
        {
            if(!Files.exists(p, LinkOption.NOFOLLOW_LINKS))
            {
                log.info("Creating local directory " + p);
                Files.createDirectories(p);
            }
        }
        catch (IOException e)
        {
            throw new IORtException(e);
        }
    }
    
}
