package org.sb.jgdrive;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.ParentReference;

public class Push implements Cmd
{
    private static final Logger log = Logger.getLogger(Push.class.getPackage().getName());

    public void exec(final Driver driver, final List<String> opts) throws IOException, IllegalStateException
    {   
        RemoteIndex ri = driver.getRemoteIndex();
        Path home = driver.getHome();
        LocalChanges lc = new LocalChanges(driver);
        
        ConcurrentHashMap<Path, File> newPathFileMap = new ConcurrentHashMap<Path, File>();
        if(!lc.isEmpty())
        {
            long largestChangeId = driver.getLargestChangeId();
            if(largestChangeId > ri.getLastRevisionId())
                throw new IllegalStateException("Remote revision is at: " + largestChangeId + ", local still at: " 
                            + ri.getLastRevisionId() + ", please pull to update local before pushing.");
            try
            {
                log.info("Pushing local changes to drive");
                Map<Path, String> modifiedFiles = lc.getModifiedFiles();
                modifiedFiles.entrySet().stream().parallel()
                    .forEach(Try.uncheck(e -> driver.updateFile(e.getValue(), home.resolve(e.getKey()))));

                driver.clearLocalChanges();
                ri.setLastSyncTime();
                
                Set<Path> newFiles = lc.getNewFiles();
                Map<Path, String> deletedPaths = lc.getDeletedPaths();
                Map<Entry<Path, String>, Path> movedFilesFromTo = lc.getMovedFilesFromTo();
                
                Map<Path, Optional<String>> pathParentIdMap = getParentIds(ri::getFileId, 
                                        Stream.concat(newFiles.stream(), movedFilesFromTo.values().stream()));
                Stream<Path> newDirs = pathParentIdMap.entrySet().stream()
                                            .filter(e -> !newFiles.contains(e.getKey()) 
                                                                && !movedFilesFromTo.containsValue(e.getKey()))
                                            .map(e -> e.getKey());
                newPathFileMap.putAll(driver.mkdirs(newDirs, pathParentIdMap::get));
                
                driver.patchFiles(
                    movedFilesFromTo.entrySet().stream().map(es -> {
                        File f = new File();
                        Path from = es.getKey().getKey();
                        Path to = es.getValue();
                        f.setId(es.getKey().getValue());
                        if(!Objects.equals(from.getFileName(), to.getFileName())) 
                            f.setTitle(to.getFileName().toString());
                        if(!Objects.equals(from.getParent(), to.getParent()))
                            f.setParents(Collections.singletonList(new ParentReference().setId(
                                        pathParentIdMap.get(to).orElseGet(() -> newPathFileMap.get(to.getParent()).getId()))));
                        return f;
                    }));
                ri.setLastSyncTime();
                
                newFiles.stream().parallel().forEach(Try.uncheck(p -> 
                        newPathFileMap.put(p, driver.insertFile(home.resolve(p), p.getParent() != null ? 
                                pathParentIdMap.get(p).orElseGet(() -> newPathFileMap.get(p.getParent()).getId()) : null))));
                
                ri.setLastSyncTime();
                
                driver.trashFiles(deletedPaths.values().stream());
                ri.removePaths(deletedPaths.keySet().stream());
                ri.setLastSyncTime();
                
            }
            finally
            {
                largestChangeId = driver.getLargestChangeId();
                if(largestChangeId > ri.getLastRevisionId())
                {
                    ri.add(newPathFileMap.values().stream());
                    ri.setLastRevisionId(largestChangeId);
                    driver.saveRemoteIndex();
                    log.info("Updated to revision: " + ri.getLastRevisionId() + ", sync time: '" + 
                            new Date(ri.getLastSyncTime().toMillis()) + "' (" + ri.getLastSyncTime() + ")");
                }
            }
        }
        else
            log.info("Not local changes found ... nothing to push");
    
    }
    
    private Map<Path, Optional<String>> getParentIds(Function<Stream<Path>, Map<Path, String>> getFileId, Stream<Path> newFiles)
    {
        Map<Path,Optional<Path>> fileParMap = newFiles.collect(Collectors.toMap(p -> p, p -> Optional.ofNullable(p.getParent())));
        Set<Path> pars = fileParMap.values().stream().filter(op -> op.isPresent()).map(op -> op.get()).collect(Collectors.toSet());
        Map<Path, String> mapPars = getFileId.apply(pars.stream());
        Map<Path, Optional<String>> currMap = fileParMap.entrySet().stream().collect(
                                Collectors.toMap(e -> e.getKey(), e -> e.getValue()
                                            .map(p -> Optional.ofNullable(mapPars.get(p))).orElse(Optional.of("root"))));
        pars.removeAll(mapPars.keySet());
        if(!pars.isEmpty())
        {
            currMap.putAll(getParentIds(getFileId, pars.stream()));
        }
        return currMap;
    }
}
