package org.sb.jgdrive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Reset implements Cmd
{
    private static final Logger log = Logger.getLogger(Reset.class.getPackage().getName());

    public void exec(final Driver driver, final List<String> opts) throws IOException, IllegalStateException
    {
        Path home = driver.getHome();
        LocalChanges lc = new LocalChanges(driver);

        Map<Path, String> deletedPaths = lc.getDeletedPaths();
        Map<Path, String> modifiedFiles = lc.getModifiedFiles();
        Map<String, Path> resetFiles = driver.downloadByFileIds(
                                            Stream.concat(modifiedFiles.values().stream(), deletedPaths.values().stream()))
                                       .collect(Collectors.toMap(e -> e.getKey().getId(), e -> e.getValue()));
        
        log.fine("Resetting files ...");
        Stream.of(modifiedFiles.entrySet(), deletedPaths.entrySet()).flatMap(es -> es.stream()).forEach(e -> 
            {
                Path tmpSrc = resetFiles.get(e.getValue());
                if(tmpSrc == null) //it is a dir
                    createDir(home.resolve(e.getKey()));
                else
                    moveFile(tmpSrc, home.resolve(e.getKey()));
                
            });
        driver.clearLocalChanges();
        
        lc.getMovedFilesFromTo().entrySet().stream().forEach(e -> moveFile(home.resolve(e.getValue()), home.resolve(e.getKey().getKey())));
        
        lc.getNewFiles().stream().map(p -> home.resolve(p)).forEach(p -> {
            try
            {
                log.info("Deleting '" + p + "'");
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
            Path parent = lp.getParent();
            if(Files.notExists(parent))
            {
                log.info("Restoring '" + parent + "'");
                Files.createDirectories(parent);
            }
            log.info("Restoring '" + lp + "'");
            Files.move(tmpPath, lp, StandardCopyOption.REPLACE_EXISTING);
        }
        catch (IOException e)
        {
            throw new IORtException(e);
        }
    }
    
    private boolean createDir(Path p)
    {
        try
        {
            if(!Files.exists(p, LinkOption.NOFOLLOW_LINKS))
            {
                log.info("Restoring '" + p + "'");
                Files.createDirectories(p);
                return true;
            }
            return false;
        }
        catch (IOException e)
        {
            throw new IORtException(e);
        }
    }    
}
