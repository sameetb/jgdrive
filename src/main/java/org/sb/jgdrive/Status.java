package org.sb.jgdrive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Status implements Cmd
{
    private static final Logger log = Logger.getLogger(Status.class.getPackage().getName());

    public void exec(final Driver driver, final List<String> opts) throws IOException, IllegalStateException
    {
        RemoteIndex ri = driver.getRemoteIndex();
        Path home = driver.getHome();
        Set<Path> localChanges = driver.getLocalModifiedFiles().collect(Collectors.toSet());

        Map<Path, String> localChangeMap = ri.getFileId(localChanges);
        
        Set<Path> newFiles = localChanges.stream().filter(p -> !localChangeMap.containsKey(p)).collect(Collectors.toSet());
        Map<Path, String> deletedPaths = ri.getFileId(ri.localPaths()
                .filter(p -> Files.notExists(home.resolve(p), LinkOption.NOFOLLOW_LINKS)).collect(Collectors.toSet()));
        
        Map<Path, Path> movedFilesFromTo = Push.detectMovedFiles(home, newFiles, deletedPaths);
        
        newFiles.removeAll(movedFilesFromTo.values());
        deletedPaths.keySet().removeAll(movedFilesFromTo.keySet());

        System.out.println("Local changes after '" + new Date(ri.getLastSyncTime().toMillis()) + "' (" + ri.getLastSyncTime() + "):");
        localChangeMap.keySet().stream().forEach(p -> System.out.println("modified: " + p));
        newFiles.stream().forEach(p -> System.out.println("added: " + p));
        deletedPaths.keySet().stream().forEach(p -> System.out.println("deleted: " + p));
        movedFilesFromTo.entrySet().stream().forEach(es -> System.out.println("moved: " + es.getKey() + " to " + es.getValue()));
        
        RemoteChanges remCh = driver.getRemoteChanges(ri.getLastRevisionId(), Optional.of(ri.getLastSyncTime()));
        System.out.println("Remote changes after change-id " + ri.getLastRevisionId() + ":");
        Stream.concat(remCh.getModifiedDirs(), remCh.getModifiedFiles()).map(f -> ri.getLocalPath(f.getId()))
                                                                    .forEach(p -> System.out.println("modified: " + p));
        Stream.concat(remCh.getDeletedDirs(), remCh.getDeletedFiles()).map(f -> ri.getLocalPath(f.getId()))
                                                                    .forEach(p -> System.out.println("deleted: " + p));
    }
}
