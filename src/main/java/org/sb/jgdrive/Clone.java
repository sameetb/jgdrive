package org.sb.jgdrive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;

public class Clone
{   
    private static final Logger log = Logger.getLogger(Clone.class.getPackage().getName());
    private final Driver driver;
    
    public Clone(Path home, boolean simulation) throws IOException, IllegalStateException
    {
        CredHelper credHelper = CredHelper.makeCredHelper(home);
        Credential cred = credHelper.get().orElseGet(() -> {
                                                            try
                                                            {
                                                                return credHelper.authorize();
                                                            }
                                                            catch(IOException io)
                                                            {
                                                                throw new IORtException(io);
                                                            }
                                                        });
        driver = new Driver(home, new Drive.Builder(credHelper.httpTransport, 
                                    credHelper.jfac, cred).setApplicationName(Driver.appName).build(), simulation);
    }
    
    public void exec(final List<String> opts) throws IOException
    {
        final boolean isDown = !opts.contains("no-download");

        Path home = driver.getHome();
        
        RemoteIndex ri = driver.getRemoteIndex();
        ri.add(driver.getAllDirs());
        
        driver.getAllFiles().forEach(s -> 
        {
            List<File> files = s.get();
            Map<File, Path> mapPath = ri.add(files.stream());
            if(isDown) 
                driver.downloadFiles(files.stream().parallel()).forEach(e -> 
                    Optional.ofNullable(mapPath.get(e.getKey())).map(dst -> moveFile(e.getValue(), home.resolve(dst))));
        });
        
        ri.setLastRevisionId(driver.getLargestChangeId());
        ri.setLastSyncTime();
        driver.saveRemoteIndex();
        log.info("Cloned to revision: " + ri.getLastRevisionId());
    }
    
    private static Path moveFile(Path tmpPath, Path lp)
    {
        try
        {
            Path parent = lp.getParent();
            if(Files.notExists(parent))
            {
                log.info("created: " + parent);
                Files.createDirectories(parent);
            }
            log.info("created: " + lp);
            return Files.move(tmpPath, lp, StandardCopyOption.REPLACE_EXISTING);
        }
        catch (IOException e)
        {
            throw new IORtException(e);
        }
    }

    public Driver getDriver()
    {
        return driver;
    }    
    
    public static List<String> help(String name)
    {
        return Collections.singletonList(name + "\t not implemented");
    }
}
