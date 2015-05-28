package org.sb.jgdrive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.ParentReference;

public class Push
{
    private static final Logger log = Logger.getLogger(Push.class.getPackage().getName());
    private final Driver driver;
    
    public Push(Driver driver)
    {
        this.driver = driver;
    }

    public void exec() throws IOException, IllegalStateException
    {   
        RemoteIndex ri = driver.getRemoteIndex();
        Path home = driver.getHome();
        Set<Path> modifiedFiles = driver.getLocalModifiedFiles().collect(Collectors.toSet());
        ri.setLastSyncTime();
        
        ConcurrentHashMap<Path, File> pathFileMap = new ConcurrentHashMap<Path, File>();
        if(modifiedFiles.size() > 0)
        {
            try
            {
                long largestChangeId = driver.getLargestChangeId();
                if(largestChangeId > ri.getLastRevisionId())
                    throw new IllegalStateException("Remote revision is at: " + largestChangeId + ", local still at: " 
                                + ri.getLastRevisionId() + ", please pull to update local before pushing.");
                
                Map<Path, String> map = ri.getFileId(modifiedFiles);
                map.entrySet().stream().parallel().forEach(e -> driver.updateFile(e.getValue(), home.resolve(e.getKey())));
                
                Set<Path> newFiles = modifiedFiles.stream().filter(p -> !map.containsKey(p)).collect(Collectors.toSet());
                Map<Path, String> deletedPaths = ri.getFileId(ri.localPaths()
                        .filter(p -> Files.notExists(home.resolve(p), LinkOption.NOFOLLOW_LINKS)).collect(Collectors.toSet()));
                
                Map<Path, String> movedFiles = detectMovedFiles(home, newFiles, deletedPaths);
                
                Map<Path, Optional<String>> pathParentIdMap = getParentIds(sp -> ri.getFileId(sp), newFiles);
                Stream<Path> newDirs = pathParentIdMap.entrySet().stream().filter(e -> !newFiles.contains(e.getKey()))
                        .map(e -> e.getKey());
                pathFileMap.putAll(driver.mkdirs(newDirs, p -> pathParentIdMap.get(p)));
                
                movedFiles.entrySet().stream().map(es -> {
                    File f = new File();
                    Path p = es.getKey();
                    f.setId(es.getValue());
                    f.setTitle(p.getFileName().toString());
                    f.setParents(Collections.singletonList(new ParentReference().setId(
                                    pathParentIdMap.get(p).orElseGet(() -> pathFileMap.get(p.getParent()).getId()))));
                    return f;
                });
                newFiles.removeAll(movedFiles.keySet());
                
                newFiles.stream().parallel().forEach(p -> 
                        pathFileMap.put(p, driver.insertFile(home.resolve(p), p.getParent() != null ? 
                                pathParentIdMap.get(p).orElseGet(() -> pathFileMap.get(p.getParent()).getId()) : null)));
                
                driver.trashFiles(deletedPaths.values().stream());
                ri.remove(deletedPaths.values().stream());
            }
            finally
            {
                long largestChangeId = driver.getLargestChangeId();
                if(largestChangeId > ri.getLastRevisionId())
                {
                    ri.add(pathFileMap.values().stream());
                    ri.setLastRevisionId(largestChangeId);
                    driver.saveRemoteIndex();
                }
            }
        }
        else
            log.info("Not local changes found ... nothing to push");
    
    }
    
    private Map<Path, String> detectMovedFiles(Path home, Set<Path> newFiles, Map<Path, String> deletedPaths)
    {
        final Function<Path, byte[]> md5HashFunc = new Function<Path, byte[]>()
        {
            private final HashMap<Path, byte[]> pathMd5Map = new HashMap<>();
            @Override
            public byte[] apply(Path path)
            {
                byte[] hash = pathMd5Map.get(path);
                if(hash == null) pathMd5Map.put(path, hash = md5Hash(path));
                return hash;
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
        
        List<SimpleImmutableEntry<Path, Path>> movedFilesFromTo =   
            newFiles.stream().map(path -> 
                deletedPaths.keySet().stream().filter(p -> !Files.isDirectory(home.resolve(p)) &&
                                                            Arrays.equals(md5HashFunc.apply(home.resolve(path)), 
                                                                                    md5HashFunc.apply(home.resolve(p))))
                                 .findFirst().<SimpleImmutableEntry<Path, Path>>map(p -> new SimpleImmutableEntry<Path, Path>(p, path)))
                         .filter(sie -> sie.isPresent()).map(sie -> sie.get()).collect(Collectors.toList());
        
        return movedFilesFromTo.stream().map(sie -> new SimpleImmutableEntry<Path, String>(sie.getValue(), 
                                                                                    deletedPaths.remove(sie.getKey())))
                                .collect(Collectors.toMap(sie -> sie.getKey(), sie -> sie.getValue()));
    }

    private Map<Path, Optional<String>> getParentIds(Function<Set<Path>, Map<Path, String>> getFileId, Set<Path> newFiles)
    {
        Map<Path,Optional<Path>> fileParMap = newFiles.stream().collect(Collectors.toMap(p -> p, p -> Optional.ofNullable(p.getParent())));
        Set<Path> pars = fileParMap.values().stream().filter(op -> op.isPresent()).map(op -> op.get()).collect(Collectors.toSet());
        Map<Path, String> mapPars = getFileId.apply(pars);
        Map<Path, Optional<String>> currMap = fileParMap.entrySet().stream().collect(
                                Collectors.toMap(e -> e.getKey(), e -> e.getValue()
                                            .map(p -> Optional.ofNullable(mapPars.get(p))).orElse(Optional.of("root"))));
        pars.removeAll(mapPars.keySet());
        if(!pars.isEmpty())
        {
            currMap.putAll(getParentIds(getFileId, pars));
        }
        return currMap;
    }
}
