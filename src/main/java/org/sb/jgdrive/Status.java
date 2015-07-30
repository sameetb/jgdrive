package org.sb.jgdrive;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.api.services.drive.model.File;

public class Status implements Cmd
{
    private static final Logger log = Logger.getLogger(Status.class.getPackage().getName());

    public void exec(final Driver driver, final List<String> opts) throws IOException, IllegalStateException
    {
        RemoteIndex ri = driver.getRemoteIndex();
        LocalChanges lc = new LocalChanges(driver);

        Stream<String> output = 
        Stream.concat(Stream.concat(Stream.concat(lc.getModifiedFiles().keySet().stream().map(p -> "modified: '" + p + "'"),
                                                                        lc.getNewFiles().stream().map(p -> "added: '" + p + "'")), 
                                                  lc.getDeletedPaths().keySet().stream().map(p -> "deleted: '" + p + "'")),
                     lc.getMovedFilesFromTo().entrySet().stream().map(es -> "moved: '" + es.getKey().getKey() + "' to '" + es.getValue() + "'"))
             .map(str -> "local " + str);
        
        String sep = System.lineSeparator();
        log.info(mkString("Local changes after '" + new Date(ri.getLastSyncTime().toMillis()) + "' (" + ri.getLastSyncTime() + "):", output, sep));
        
        if(!opts.contains("local-only"))
        {
            boolean full = opts.contains("full");
            Map<String, String> nvpFlags = Cmd.nvpFlags(opts.stream());
            long revisionId = Optional.ofNullable(nvpFlags.get("revision")).map(str -> Long.parseLong(str)).orElseGet(() -> ri.getLastRevisionId());
            Optional<FileTime> syncTime = Optional.of(nvpFlags.get("revision") == null).filter(ne -> ne == true).map(ne -> ri.getLastSyncTime());
            RemoteChanges remCh = driver.getRemoteChanges(revisionId, syncTime);
            
            Map<String, File> modMap = Stream.concat(remCh.getModifiedDirs(), remCh.getModifiedFiles()).collect(Collectors.toMap(f -> f.getId(), f -> f));
            Map<String, Path> modPathMap = ri.getLocalPath(modMap.keySet()).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

            Map<String, File> delMap = Stream.concat(remCh.getDeletedDirs(), remCh.getDeletedFiles()).collect(Collectors.toMap(f -> f.getId(), f -> f));
            Map<String, Path> delPathMap = ri.getLocalPath(delMap.keySet()).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (u, v) -> u));
            
            output = Stream.concat(
                        modMap.keySet().stream().map(id -> modStatus(Optional.ofNullable(modPathMap.get(id)), modMap.get(id), full)),
                        delMap.keySet().stream().map(id -> delStatus(Optional.ofNullable(delPathMap.get(id)), delMap.get(id), full)))
                    .map(str -> "remote " + str);
            log.info(mkString("Remote changes after change-id " + revisionId + ":", output, sep));
        }
    }

    private String modStatus(Optional<Path> path, File f, boolean full)
    {
        return status(path, f, full, "modified", "new");
    }

    private String delStatus(Optional<Path> path, File f, boolean full)
    {
        return status(path, f, full, "deleted", "deleted non-existent");
    }

    private String status(Optional<Path> path, File f, boolean full, String msg1, String msg2)
    {
        return mkString(path.map(p -> msg1 + ": '" + p.toString()).orElse(msg2 + ": '" + f.getTitle()) + "'", 
                    full ? Stream.of(f.toString()): Stream.empty(), ", ");
    }
    
    private String mkString(String base, Stream<String> msgs, String sep)
    {
        return msgs.reduce(new StringBuilder(base), (sb, s2) -> sb.append(sep).append(s2), 
                (sb1, sb2) -> sb1.append(sep).append(sb2)).toString();
    }
}
