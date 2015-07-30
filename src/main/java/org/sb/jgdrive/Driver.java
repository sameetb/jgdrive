package org.sb.jgdrive;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.security.GeneralSecurityException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.batch.BatchCallback;
import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonGenerator;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.Drive.Changes;
import com.google.api.services.drive.model.About;
import com.google.api.services.drive.model.Change;
import com.google.api.services.drive.model.ChangeList;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.drive.model.ParentReference;

public class Driver
{
    static final String MIME_TYPE_DIR = "application/vnd.google-apps.folder";
    private static final String FILE_ATTRS = "id,title,parents(id),version,mimeType,modifiedDate,md5Checksum,labels(trashed)";
    private static final Logger log = Logger.getLogger(Driver.class.getPackage().getName());
    static final JsonFactory jfac = JacksonFactory.getDefaultInstance();
    static final String appName = "jgdrive";
    private final Path home;
    private final Supplier<RemoteIndex> ri;
    private final Supplier<Drive> drive;
    private final Supplier<HashSet<String>> li = makeLocalIndex();
    private final boolean simulation;
    private final Supplier<Pattern[]> ignores = CachingSupplier.wrap(() -> readIgnores().map(Pattern::compile).toArray(Pattern[]::new));
    
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
                /*FlatRemoteIndex fri = jfac.fromReader(new FileReader(riPath.toFile()), FlatRemoteIndex.class);
                return new RemoteIndex(fri.lastSyncTimeEpochMillis, fri.lastRevisionId, fri.entryById);*/
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
            throw new IllegalStateException("The path '" + riPath + "' must be a directory.");
        this.ri = CachingSupplier.wrap(() -> {   try
                            {
                                Entry<Long, String> pair = getLargestChangeIdAndRootFolderId();
                                return new RemoteIndex(pair.getKey(), pair.getValue());
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
        if(!simulation)
        {
            FileWriter fw = new FileWriter(riPath().toFile());
            JsonGenerator gen = jfac.createJsonGenerator(fw);
            gen.serialize(ri.get());
            gen.flush();
            fw.close();
        }
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
    
    public Stream<Path> getLocalModifiedFiles(Optional<FileTime> fromTime) throws IOException
    {
        Path idxDir = jgdrive();
        RemoteIndex idx = getRemoteIndex();
        FileTime lastSyncTime = fromTime.orElseGet(() -> idx.getLastSyncTime());
        Pattern[] igns = ignores.get();
        return Stream.concat(li.get().stream().map(s -> Paths.get(s)), 
                Files.find(home, Integer.MAX_VALUE, 
                    (p, a) -> !p.startsWith(idxDir) 
                                && Stream.of(igns).noneMatch(pat -> pat.asPredicate().test(p.getFileName().toString()))
                                && a.isRegularFile()
                                && (a.lastModifiedTime().compareTo(lastSyncTime) > 0 || !idx.exists(home.relativize(p))))
                     .map(p -> home.relativize(p)));
    }
    
    public Stream<Entry<File, Path>> downloadByFileIds(Stream<String> fileIds) throws IORtException, IOException
    {
        ArrayList<File> res = new ArrayList<>(); 
        BatchRequest batch = drive.get().batch();
        final Callback<File> cb = new Callback<File>()
        {
            @Override
            public void onSuccess(File t, HttpHeaders responseHeaders) throws IOException
            {
                res.add(t);
            }
        };
        
        try
        {
            com.google.api.services.drive.Drive.Files files = drive.get().files();
            fileIds.forEach(f -> {
                try
                {
                    batch.queue(files.get(f).setFields(FILE_ATTRS).buildHttpRequest(), File.class, GoogleJsonError.class, cb);
                }
                catch(IOException io)
                {
                    throw new IORtException(io);
                }
            });
            if(batch.size() > 0) batch.execute();
        }
        catch (IORtException io)
        {
            throw (IOException)io.getCause();
        }
        
        return Stream.concat(
                res.stream().filter(f -> f.getMimeType().equals(MIME_TYPE_DIR)).map(f -> new SimpleImmutableEntry<>(f, null)),
                downloadFiles(res.stream().filter(f -> !f.getMimeType().equals(MIME_TYPE_DIR))));
    }
    
    public Stream<Entry<File, Path>> downloadFiles(Stream<File> files) throws IORtException
    {
        return 
        files.map(f ->  {try
                         {
                            Path path = Files.createTempFile("jgdrive-", "-" + f.getId() + "-" + f.getTitle());
                            log.fine("Downloading " + f.getId() + " to " + path + " ....");
                            // uses alt=media query parameter to request content
                            ChecksumInputStream remStream = new ChecksumInputStream(drive.get().files().get(f.getId()).executeMediaAsInputStream());
                            long size = Files.copy(remStream, path, StandardCopyOption.REPLACE_EXISTING);
                            remStream.close();
                            if(!f.getMd5Checksum().equals(remStream.getHexChecksum()))
                                throw new RuntimeException("Checksum verification failed for " 
                                            + "id=" + f.getId() + ", title=" + f.getTitle() + ", expected=" + f.getMd5Checksum() + ", actual=" + remStream.getHexChecksum());
                            Files.setLastModifiedTime(path, FileTime.fromMillis(f.getModifiedDate().getValue()));
                            log.fine("Downloaded " + f.getId() + " to " + path + ", size=" + size + " bytes");
                            return new SimpleImmutableEntry<>(f, path);
                         }
                         catch (IOException e)
                         {
                             throw new IORtException(e);
                         }});
    }    
    
    public void updateFile(String fileId, Path localPath) throws IORtException
    {
        try
        {
            info("Updating remote '" + localPath + "'", " with id = " + fileId);
            if(!simulation)
            {
                ChecksumInputStream fileStream = new ChecksumInputStream(Files.newInputStream(localPath, StandardOpenOption.READ));
                InputStreamContent mediaContent = new InputStreamContent(mime(localPath), fileStream);
                // Send the request to the API.
                File file = drive.get().files().update(fileId, null, mediaContent).setFields(FILE_ATTRS).execute();
                if(!fileStream.getHexChecksum().equalsIgnoreCase(file.getMd5Checksum()))
                    throw new IOException("Failed to verify md5 checksum for id=" + file.getId() + ", path=" + localPath
                                        + ", expected=" + fileStream.getHexChecksum() + ", found=" + file.getMd5Checksum());
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
    
            info("Adding remote '" + localDir + "'", " with parent " + body.getParents());
            if(!simulation)
            try
            {
                body = drive.get().files().insert(body).setFields(FILE_ATTRS).execute();
            }
            catch (IOException e)
            {
                throw new IORtException(e);
            }
            else
                body.setId(String.valueOf(new Random().nextInt()));
            pathFileMap.put(localDir, body);
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

        info("Adding remote '" + localPath + "'", " with parent " + body.getParents());
        
        if(!simulation)
        try
        {
            ChecksumInputStream fileStream = new ChecksumInputStream(Files.newInputStream(localPath, StandardOpenOption.READ));
            InputStreamContent mediaContent = new InputStreamContent(body.getMimeType(), fileStream);
            File file = drive.get().files().insert(body, mediaContent).setFields(FILE_ATTRS).execute();
            if(!fileStream.getHexChecksum().equalsIgnoreCase(file.getMd5Checksum()))
                throw new IORtException("Failed to verify md5 checksum for id=" + file.getId() + ", path=" + localPath
                                    + ", expected=" + fileStream.getHexChecksum() + ", found=" + file.getMd5Checksum());
            return file;
        }
        catch (IOException e)
        {
            throw new IORtException(e);
        }
        else
            return body.setId(String.valueOf(new Random().nextInt()));
    }

    public void trashFiles(Stream<String> fileIds) throws IOException
    {
        BatchRequest batch = drive.get().batch();
        final Callback<File> cb = new Callback<File>()
        {
            @Override
            public void onSuccess(File t, HttpHeaders responseHeaders)
            {
            }
        };
        
        com.google.api.services.drive.Drive.Files files = drive.get().files();
        fileIds.forEach(fileId -> {try
        {
            log.info("Trashing remote '" + fileId + "'");
            batch.queue(files.trash(fileId).setFields(FILE_ATTRS).buildHttpRequest(), File.class, GoogleJsonError.class, cb);
        }
        catch (IOException e)
        {
            throw new IORtException(e);
        }});
        if(!simulation && batch.size() > 0) batch.execute();
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
    
    private Entry<Long, String> getLargestChangeIdAndRootFolderId() throws IOException
    {
        //setFields("largestChangeId,name,user,rootFolderId,languageCode")
        About abt = drive.get().about().get().setFields("largestChangeId,rootFolderId").execute();
        return new SimpleImmutableEntry<>(abt.getLargestChangeId(), abt.getRootFolderId());
    }

    public long getLargestChangeId() throws IOException
    {
        //setFields("largestChangeId,name,user,rootFolderId,languageCode")
        return drive.get().about().get().setFields("largestChangeId").execute().getLargestChangeId();
    }
    
    public Stream<File> getAllDirs() throws IOException
    {
        return StreamSupport.stream(
        new Spliterators.AbstractSpliterator<File>(Long.MAX_VALUE, Spliterator.SIZED)
        {
            final Drive.Files.List request = drive.get().files().list()
                    .setQ("trashed = false and 'me' in owners and mimeType = '" + MIME_TYPE_DIR + "'")
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
                    System.out.println("Read size=" + list.getItems().size());
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
                    System.out.println("..done ");
                    return false;
                }
            }
        }, false);
    }

    public Map<String, File> getFiles(Stream<String> fileIds) throws IOException
    {
        HashMap<String, File> res = new HashMap<>(); 
        BatchRequest batch = drive.get().batch();
        final Callback<File> cb = new Callback<File>()
        {
            @Override
            public void onSuccess(File t, HttpHeaders responseHeaders) throws IOException
            {
                res.put(t.getId(), t);
            }
        };
        
        try
        {
            com.google.api.services.drive.Drive.Files files = drive.get().files();
            fileIds.forEach(f -> {
                try
                {
                    batch.queue(files.get(f).setFields(FILE_ATTRS).buildHttpRequest(), File.class, GoogleJsonError.class, cb);
                }
                catch(IOException io)
                {
                    throw new IORtException(io);
                }
            });
            if(batch.size() > 0) batch.execute();
        }
        catch (IORtException io)
        {
            throw (IOException)io.getCause();
        }
        return res;
    }
    
    public Stream<Supplier<List<File>>> getAllFiles() throws IOException
    {
        return StreamSupport.stream(
        new Spliterators.AbstractSpliterator<Supplier<List<File>>>(Long.MAX_VALUE, Spliterator.SIZED)
        {
            final Drive.Files.List request = drive.get().files().list()
                    .setQ("trashed = false and 'me' in owners and mimeType != '" + MIME_TYPE_DIR + "'")
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
    
    public void patchFiles(Stream<File> files) throws IOException
    {
        BatchRequest batch = drive.get().batch();
        final Callback<Void> cb = new Callback<Void>()
        {
            @Override
            public void onSuccess(Void t, HttpHeaders responseHeaders) throws IOException
            {
            }
            
        };

        com.google.api.services.drive.Drive.Files dfiles = drive.get().files();
        files.forEach(f -> {
            try
            {
                log.info("Updating remote meta-data of '" + f.getId() + "'");
                batch.queue(dfiles.patch(f.getId(), f).setFields(FILE_ATTRS).buildHttpRequest(), Void.class, GoogleJsonError.class, cb);
            }
            catch (IOException e)
            {
                throw new IORtException(e);
            }
        });
        if(!simulation && batch.size() > 0) batch.execute();
    }
    
    private static abstract class Callback<T> implements BatchCallback<T, GoogleJsonError> 
    {
        @Override
        public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) throws IOException
        {
            throw new HttpResponseException.Builder(e.getCode(), e.getMessage(), responseHeaders)
                                                                        .setContent(e.toPrettyString()).build();
        }
    }
    
    private void info(String info, String fine)
    {
        if(log.isLoggable(Level.FINE)) log.fine(info + fine);
        else log.info(info);
    }
    
    private Path liPath()
    {
        Path jgdrive = jgdrive();
        return jgdrive.resolve("local_index.json");
    }
    
    private Supplier<HashSet<String>> makeLocalIndex()
    {
       return CachingSupplier.wrap(() -> {
            try
            {
                Path riPath = liPath();
                if (!Files.exists(riPath))
                    return new HashSet<>();
                log.fine("Reading local index from " + riPath);
                return jfac.fromReader(new FileReader(riPath.toFile()), HashSet.class);
            }
            catch (IOException e)
            {
                throw new IORtException(e);
            }
        });
    }

    public void saveLocalChanges(Set<Path> locs) throws IOException
    {
        HashSet<String> hs = li.get();
        locs.stream().forEach(p -> hs.add(p.toString()));
        saveLocalIndex();
    }
    
    public void clearLocalChanges() throws IOException
    {
        li.get().clear();
        saveLocalIndex();
    }
    
    private void saveLocalIndex() throws IOException
    {
        if(!simulation)
        {
            FileWriter fw = new FileWriter(liPath().toFile());
            JsonGenerator gen = jfac.createJsonGenerator(fw);
            gen.serialize(li.get());
            gen.flush();
            fw.close();
        }
    }
    
    private Stream<String> readIgnores()
    {
        Path jgdrive = jgdrive();
        Path ignPath = jgdrive.resolve("ignore.txt");
        if (Files.exists(ignPath))
            try
            {
                return Files.readAllLines(ignPath).stream();
            }
            catch (IOException e)
            {
                log.log(Level.WARNING, "Failed to read ignores file " + ignPath, e);
            }
        return Stream.of("^\\.~.*#$", "^~\\$.*");
    }
}
