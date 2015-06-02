package org.sb.jgdrive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.api.services.drive.model.File;

public class Reset implements Cmd
{
    private static final Logger log = Logger.getLogger(Reset.class.getPackage().getName());

    public void exec(final Driver driver, final List<String> opts) throws IOException, IllegalStateException
    {
        RemoteIndex ri = driver.getRemoteIndex();
        Path home = driver.getHome();
        Set<Path> localChanges = driver.getLocalModifiedFiles().collect(Collectors.toSet());

        Map<Path, String> map = ri.getFileId(localChanges);
        
        Set<Path> newFiles = localChanges.stream().filter(p -> !map.containsKey(p)).collect(Collectors.toSet());
        Map<Path, String> deletedPaths = ri.getFileId(ri.localPaths()
                .filter(p -> Files.notExists(home.resolve(p), LinkOption.NOFOLLOW_LINKS)).collect(Collectors.toSet()));
        
        Map<Path, Path> movedFilesFromTo = Push.detectMovedFiles(home, newFiles, deletedPaths);
        
        newFiles.removeAll(movedFilesFromTo.values());
        deletedPaths.keySet().removeAll(movedFilesFromTo.keySet());
        
        Map<File, Path> resetFiles = driver.downloadByFileIds(
                                Stream.concat(map.values().stream(), deletedPaths.values().stream()).parallel());
        
        log.fine("Resetting files ...");
        resetFiles.entrySet().stream().forEach(e -> 
                ri.getLocalPath(e.getKey().getId()).ifPresent(lp -> moveFile(e.getValue(), home.resolve(lp))));
        
        movedFilesFromTo.entrySet().stream().forEach(e -> moveFile(home.resolve(e.getValue()), home.resolve(e.getKey())));
        
        newFiles.stream().map(p -> home.resolve(p)).forEach(p -> {
            try
            {
                log.info("Deleting 'new' file " + p);
                Files.deleteIfExists(p);
            }
            catch(IOException e)
            {
                throw new IORtException(e);
            }
        });
    }
    
    private void moveFile(Path tmpPath, Path lp)
    {
        try
        {
            log.info("Resetting local file " + lp + " with " + tmpPath);
            Files.move(tmpPath, lp, StandardCopyOption.REPLACE_EXISTING);
        }
        catch (IOException e)
        {
            throw new IORtException(e);
        }
    }
}
