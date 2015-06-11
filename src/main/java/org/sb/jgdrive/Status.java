package org.sb.jgdrive;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class Status implements Cmd
{
    private static final Logger log = Logger.getLogger(Status.class.getPackage().getName());

    public void exec(final Driver driver, final List<String> opts) throws IOException, IllegalStateException
    {
        RemoteIndex ri = driver.getRemoteIndex();
        LocalChanges lc = new LocalChanges(driver);

        Stream<String> output = 
        Stream.concat(Stream.concat(Stream.concat(lc.getModifiedFiles().keySet().stream().map(p -> "modified: " + p),
                                                                        lc.getNewFiles().stream().map(p -> "added: " + p)), 
                                                  lc.getDeletedPaths().keySet().stream().map(p -> "deleted: " + p)),
                     lc.getMovedFilesFromTo().entrySet().stream().map(es -> "moved: '" + es.getKey().getKey() + "' to '" + es.getValue() + "'"))
             .map(str -> "local " + str);
        
        String sep = System.lineSeparator();
        log.info(mkString("Local changes after '" + new Date(ri.getLastSyncTime().toMillis()) + "' (" + ri.getLastSyncTime() + "):", output, sep));
        
        if(!opts.contains("local-only"))
        {
            RemoteChanges remCh = driver.getRemoteChanges(ri.getLastRevisionId(), Optional.of(ri.getLastSyncTime()));
            output = Stream.concat(
                    Stream.concat(remCh.getModifiedDirs(), remCh.getModifiedFiles()).map(f -> "modified: " + ri.getLocalPath(f.getId())),
                    Stream.concat(remCh.getDeletedDirs(), remCh.getDeletedFiles()).map(f -> "deleted: " + ri.getLocalPath(f.getId())))
                    .map(str -> "remote " + str);
            log.info(mkString("Remote changes after change-id " + ri.getLastRevisionId() + ":", output, sep));
        }
    }

    private String mkString(String base, Stream<String> msgs, String sep)
    {
        return msgs.reduce(new StringBuilder(base), (sb, s2) -> sb.append(sep).append(s2), 
                (sb1, sb2) -> sb1.append(sep).append(sb2)).toString();
    }
}
