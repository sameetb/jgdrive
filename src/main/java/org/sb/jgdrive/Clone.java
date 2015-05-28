package org.sb.jgdrive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;

public class Clone
{   
    private static final Logger log = Logger.getLogger(Clone.class.getPackage().getName());
    private final Driver driver;
    
    public Clone(Path home, boolean simulation) throws IOException, IllegalStateException, GeneralSecurityException
    {
        Path jgdrive = home.resolve(".jgdrive");
        if(!Files.exists(jgdrive)) throw new IllegalStateException("The path '" + jgdrive.getFileName() 
                                        + "' already exists in '" + jgdrive.getParent() + "', please remove it first.");
        Files.createDirectory(jgdrive);
        final HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        final JsonFactory jfac = Driver.jfac;
        CredHelper credHelper = new CredHelper(jgdrive, httpTransport, jfac);
        Credential cred = credHelper.authorize();
        driver = new Driver(home, 
                new Drive.Builder(httpTransport, jfac, cred).setApplicationName(Driver.appName).build(), simulation);
    }
    
    public void exec() throws IOException
    {
        ArrayList<Map<File, Path>> maps = new ArrayList<>();
        
        RemoteIndex ri = driver.getRemoteIndex();
        driver.getAllFiles().forEach(s -> 
        {
            List<File> files = s.get();
            ri.add(files.stream());
            maps.add(driver.downloadFiles(files.stream().filter(f -> !isDir(f)).parallel()));
        });
        Path home = driver.getHome();
        maps.stream().forEach(map -> 
            map.entrySet().stream().forEach(e -> 
                ri.getLocalPath(e.getKey().getId()).ifPresent(lp -> moveFile(e.getValue(), home.resolve(lp)))));
    }
    
    private boolean isDir(File f)
    {
        return f.getMimeType().equals(Driver.MIME_TYPE_DIR);
    }    
    
    private static void moveFile(Path tmpPath, Path lp)
    {
        try
        {
            Path parent = lp.getParent();
            if(Files.notExists(parent))
            {
                log.info("Creating local directory " + parent);
                Files.createDirectories(parent);
            }
            log.info("Creating local file " + lp + " with " + tmpPath);
            Files.move(tmpPath, lp, StandardCopyOption.REPLACE_EXISTING);
        }
        catch (IOException e)
        {
            throw new IORtException(e);
        }
    }    
}
