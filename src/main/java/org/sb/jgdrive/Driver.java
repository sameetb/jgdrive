package org.sb.jgdrive;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.security.GeneralSecurityException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonGenerator;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.Drive.Changes;
import com.google.api.services.drive.model.Change;
import com.google.api.services.drive.model.ChangeList;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.drive.model.ParentReference;

public class Driver
{
    static final String MIME_TYPE_DIR = "application/vnd.google-apps.folder";
    private static final String FILE_ATTRS = "id,title,parents,version,mimeType,modifiedDate,md5Checksum,labels(trashed)";
    private static final Logger log = Logger.getLogger(Driver.class.getPackage().getName());
    static final JsonFactory jfac = JacksonFactory.getDefaultInstance();
    static final String appName = "jgdrive";
    private final Path home;
    private final Supplier<RemoteIndex> ri;
    private final Supplier<Drive> drive;
    private final boolean simulation;
    
    public Driver(Path home, boolean simulation) throws IllegalStateException
    {
        this.home = home;

        ri = CachingSupplier.wrap(() -> {
            try
            {
                Path riPath = riPath();
                if (!Files.exists(riPath))
                    throw new IllegalStateException("Could not find an index at " + riPath.toAbsolutePath());
                log.fine("Reading index from " + riPath);
                return jfac.fromReader(new FileReader(riPath.toFile()), RemoteIndex.class);
            }
            catch (IOException e)
            {
                throw new IORtException(e);
            }
        });
        
        drive = CachingSupplier.wrap(() -> getDrive());
        this.simulation = simulation;
    }

    public Driver(Path home, Drive drive, boolean simulation) throws IllegalStateException, IOException
    {
        this.home = home;
        this.drive = () -> drive;
        Path riPath = jgdrive();
        if (!Files.exists(riPath))
            Files.createDirectories(riPath);
        else if(!Files.isDirectory(riPath))
            throw new IllegalStateException("The path " + riPath + " must be a directory.");
        this.ri = CachingSupplier.wrap(() -> {   try
                            {
                                return new RemoteIndex(getLargestChangeId(), Stream.empty());
                            }
                            catch (IOException e)
                            {
                                throw new IORtException(e);
                            }});
        this.simulation = simulation;
    }

    public Path getHome()
    {
        return home;
    }
    
    public RemoteIndex getRemoteIndex()
    {
        return ri.get();
    }
    
    public RemoteChanges getRemoteChanges(Long lastChangeId, Optional<FileTime> modifiedDtm) throws IOException
    {
        Stream<Change> stream = Stream.empty();
        Changes.List list = drive.get().changes().list()
                        .setStartChangeId(lastChangeId + 1)
                        .setIncludeDeleted(true)
                        .setIncludeSubscribed(false)
                        .setFields("largestChangeId,nextPageToken,items(deleted,file(" + FILE_ATTRS + "))");
        Long id = null;
        do
        {
            ChangeList changes = list.execute();
            if(id == null) id = changes.getLargestChangeId();
            stream = Stream.concat(stream, changes.getItems().stream()
                        .filter(ch -> modifiedDtm.map(dt -> ch.getFile().getModifiedDate().getValue() > dt.toMillis()).orElse(true)));
            list.setPageToken(changes.getNextPageToken());
        }
        while(list.getPageToken() != null && list.getPageToken().length() > 0);
        
        return new RemoteChanges(stream, id);
    }

    private Drive getDrive() throws IORtException
    {
        try
        {
            HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
            CredHelper credHelper = new CredHelper(jgdrive(), httpTransport, jfac);
            Credential cred = credHelper.get()
                                .orElseThrow(() -> new IOException("Did not find credentials from " + jgdrive()));
            return new Drive.Builder(httpTransport, jfac,  cred).setApplicationName(appName).build();
        }
        catch(GeneralSecurityException ge)
        {
           throw new RuntimeException(ge); 
        }
        catch(IOException io)
        {
            throw new IORtException(io);
        }
    }

    public void saveRemoteIndex() throws IOException
    {
        FileWriter fw = new FileWriter(riPath().toFile());
        JsonGenerator gen = jfac.createJsonGenerator(fw);
        gen.serialize(ri);
        gen.flush();
        fw.close();    
    }

    private Path riPath()
    {
        Path jgdrive = jgdrive();
        return jgdrive.resolve("remote_index.json");
    }

    private Path jgdrive()
    {
        return home.resolve(".jgdrive");
    }
    
    public Stream<Path> getLocalModifiedFiles() throws IOException
    {
        Path idxDir = jgdrive();
        FileTime lastSyncTime = getRemoteIndex().getLastSyncTime();
        return Files.find(home, Integer.MAX_VALUE, 
                (p, a) -> !p.startsWith(idxDir) 
                            && a.isRegularFile()
                            && a.lastModifiedTime().compareTo(lastSyncTime) > 0)
                    .map(p -> home.relativize(p));
    }

    public Map<File, Path> downloadByFileIds(Stream<String> fileIds) throws IORtException
    {
        return downloadFiles(fileIds.<File>map(fileId -> {
            try
            {
                return drive.get().files().get(fileId).execute();
            }
            catch(IOException e)
            {
                throw new IORtException(e);
            }
        }));
    }
    
    public Map<File, Path> downloadFiles(Stream<File> files) throws IORtException
    {
        return 
        files.map(f ->  {try
                         {
                            Path path = Files.createTempFile("jgdrive-", "-" + f.getTitle());
                            log.fine("Downloading " + f.getId() + " to " + path + " ....");
                            // uses alt=media query parameter to request content
                            ChecksumInputStream remStream = new ChecksumInputStream(drive.get().files().get(f.getId()).executeMediaAsInputStream());
                            long size = Files.copy(remStream, path, StandardCopyOption.REPLACE_EXISTING);
                            remStream.close();
                            if(!f.getMd5Checksum().equals(remStream.getMd5HexChecksum()))
                                throw new RuntimeException("Checksum verification failed for " 
                                            + "id=" + f.getId() + ", title=" + f.getTitle() + ", expected=" + f.getMd5Checksum() + ", actual=" + remStream.getMd5HexChecksum());
                            Files.setLastModifiedTime(path, FileTime.fromMillis(f.getModifiedDate().getValue()));
                            log.fine("Downloaded " + f.getId() + " to " + path + ", size=" + size + " bytes");
                            return new SimpleImmutableEntry<>(f, path);
                         }
                         catch (IOException e)
                         {
                             throw new IORtException(e);
                         }})
              .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }    
    
    public void updateFile(String fileId, Path localPath) throws IORtException
    {
        try
        {
            log.info("Updating remote file: " + fileId + " from " + localPath);
            if(!simulation)
            {
                ChecksumInputStream fileStream = new ChecksumInputStream(Files.newInputStream(localPath, StandardOpenOption.READ));
                InputStreamContent mediaContent = new InputStreamContent(mime(localPath), fileStream);
                // Send the request to the API.
                File file = drive.get().files().update(fileId, null, mediaContent).setFields(FILE_ATTRS).execute();
                if(fileStream.getMd5HexChecksum().equalsIgnoreCase(file.getMd5Checksum()))
                    throw new IORtException("Failed to verify md5 checksum for id=" + file.getId() + ", path=" + localPath
                                        + ", expected=" + fileStream.getMd5HexChecksum() + ", found=" + file.getMd5Checksum());
            }
        }
        catch (IOException e)
        {
            throw new IORtException(e);
        }
    }
    
    public Map<Path, File> mkdirs(Stream<Path> newDirs, Function<Path, Optional<String>> pathParentIdMap) throws IORtException
    {
        LinkedHashMap<Path, File> pathFileMap = new LinkedHashMap<>();
        newDirs.sorted(Comparator.naturalOrder()).forEach(localDir ->
        {
            // File's metadata.
            File body = new File();
            body.setTitle(localDir.getFileName().toString());
            //body.setDescription(description);
            body.setMimeType(MIME_TYPE_DIR);
    
            if(localDir.getParent() != null)
            {
                String parId = pathParentIdMap.apply(localDir).orElseGet(() -> pathFileMap.get(localDir.getParent()).getId());
                body.setParents(Collections.singletonList(new ParentReference().setId(parId)));
            }
    
            log.info("Adding remote directory from " + localDir + " with parent " + body.getParents());
            if(!simulation)
            try
            {
                File file = drive.get().files().insert(body).setFields(FILE_ATTRS).execute();
                pathFileMap.put(localDir, file);
            }
            catch (IOException e)
            {
                throw new IORtException(e);
            }
            else
                pathFileMap.put(localDir, body.setId(String.valueOf(new Random().nextInt())));
        });
        return pathFileMap;
    }
    
    public File insertFile(Path localPath, String parentId) throws IORtException
    {
        // File's metadata.
        File body = new File();
        body.setTitle(localPath.getFileName().toString());
        //body.setDescription(description);
        body.setMimeType(mime(localPath));

        // Set the parent folder.
        if (parentId != null && parentId.length() > 0)
            body.setParents(Collections.singletonList(new ParentReference().setId(parentId)));

        log.info("Adding remote file from " + localPath + " with parent " + body.getParents());
        
        if(!simulation)
        try
        {
            ChecksumInputStream fileStream = new ChecksumInputStream(Files.newInputStream(localPath, StandardOpenOption.READ));
            InputStreamContent mediaContent = new InputStreamContent(body.getMimeType(), fileStream);
            File file = drive.get().files().insert(body, mediaContent).setFields(FILE_ATTRS).execute();
            if(fileStream.getMd5HexChecksum().equalsIgnoreCase(file.getMd5Checksum()))
                throw new IORtException("Failed to verify md5 checksum for id=" + file.getId() + ", path=" + localPath
                                    + ", expected=" + fileStream.getMd5HexChecksum() + ", found=" + file.getMd5Checksum());
            return file;
        }
        catch (IOException e)
        {
            throw new IORtException(e);
        }
        else
            return body.setId(String.valueOf(new Random().nextInt()));
    }

    public void trashFiles(Stream<String> fileIds) throws IORtException
    {
        fileIds.forEach(fileId -> {try
        {
            log.info("Trashing remote file: " + fileId);
            if(!simulation)drive.get().files().trash(fileId).setFields(FILE_ATTRS).execute();
        }
        catch (IOException e)
        {
            throw new IORtException(e);
        }});
    }
    
    private static String mime(Path p)
    {
        try
        {
            String type = Files.probeContentType(p);
            if(type != null) return type; 
        }
        catch (IOException e)
        {
        }
        return "application/octet-stream";
    }    
    
    public long getLargestChangeId() throws IOException
    {
        //setFields("largestChangeId,name,user,rootFolderId,languageCode")
        return drive.get().about().get().setFields("largestChangeId").execute().getLargestChangeId();
    }
    
    public Stream<File> getAllFiles2() throws IOException
    {
        return StreamSupport.stream(
        new Spliterators.AbstractSpliterator<File>(Long.MAX_VALUE, Spliterator.SIZED)
        {
            final Drive.Files.List request = drive.get().files().list()
                    .setQ("trashed = false and 'me' in owners")
                    .setFields("nextPageToken,items(" + FILE_ATTRS + ")").setMaxResults(400);
            FileList list = null;
            Iterator<File> it = null;
            
            @Override
            public boolean tryAdvance(Consumer<? super File> action)
            {
                if(list == null)
                {
                    try
                    {
                        list = request.execute();
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException(e);
                    }
                    it = list.getItems().iterator();
                }
                if(it.hasNext())
                {
                    action.accept(it.next());
                    return true;
                }
                else
                {
                    if(list.getNextPageToken() != null && list.getNextPageToken().length() > 0)
                    {
                        System.out.println("Reading token " + list.getNextPageToken());
                        request.setPageToken(list.getNextPageToken());
                        list = null;
                        return tryAdvance(action);
                    }
                    System.out.println("..done");
                    return false;
                }
            }
        }, false);
    }

    public Stream<Supplier<List<File>>> getAllFiles() throws IOException
    {
        return StreamSupport.stream(
        new Spliterators.AbstractSpliterator<Supplier<List<File>>>(Long.MAX_VALUE, Spliterator.SIZED)
        {
            final Drive.Files.List request = drive.get().files().list()
                    .setQ("trashed = false and 'me' in owners")
                    .setFields("nextPageToken,items(" + FILE_ATTRS + ")")
                    .setMaxResults(400);
            
            @Override
            public boolean tryAdvance(Consumer<? super Supplier<List<File>>> action)
            {       //first request                         or needs another request
                if(request.getLastResponseHeaders() == null || request.getPageToken() != null)
                {
                    action.accept(new Supplier<List<File>>()
                        {
                            @Override
                            public List<File> get()
                            {
                                try
                                {
                                    FileList list = request.execute();
                                    if(list.getNextPageToken() != null && list.getNextPageToken().length() > 0)
                                    {
                                        log.fine("Reading token " + list.getNextPageToken());
                                        request.setPageToken(list.getNextPageToken());
                                    }
                                    else
                                    {
                                        request.setPageToken(null);
                                        log.fine("..done reading all tokens");
                                    }
                                    return list.getItems();
                                }
                                catch (IOException e)
                                {
                                    throw new RuntimeException(e);
                                }
                            }
                        });
                    return true;
                }
                return false;
            }
        }, false);
    }
    
    public void patchFiles(Stream<File> files) throws IORtException
    {
        files.forEach(f -> {
            try
            {
                drive.get().files().patch(f.getId(), f).setFields(FILE_ATTRS).execute();
            }
            catch (IOException e)
            {
                throw new IORtException(e);
            }
        });

    }
}
