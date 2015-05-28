package org.sb.jgdrive;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.api.services.drive.model.Change;
import com.google.api.services.drive.model.File;

public class RemoteChanges
{
    private final Set<Change> changes;
    private final long largestChangeId;
    public RemoteChanges(Stream<Change> changes, long largestChangeId)
    {
        super();
        this.changes = changes.collect(Collectors.toSet());
        this.largestChangeId = largestChangeId;
    }
    
    public Set<Change> getChanges()
    {
        return changes;
    }
    public long getLargestChangeId()
    {
        return largestChangeId;
    }
    
    public Stream<File> getModifiedFiles()
    {
        return getChanges().stream()
                .filter(ch -> !isDeleted(ch) && !isDir(ch))
                .map(ch -> ch.getFile());
    }

    private boolean isDir(Change ch)
    {
        return ch.getFile().getMimeType().equals("application/vnd.google-apps.folder");
    }

    private boolean isDeleted(Change ch)
    {
        return ch.getDeleted() || ch.getFile().getLabels().getTrashed();
    }
    
    public Stream<File> getModifiedDirs()
    {
        return getChanges().stream().filter(ch -> !isDeleted(ch) && isDir(ch)).map(ch -> ch.getFile());
    }

    public Stream<File> getDeletedFiles()
    {
        return getChanges().stream().filter(ch -> isDeleted(ch) && !isDir(ch)).map(ch -> ch.getFile());
    }

    public Stream<File> getDeletedDirs()
    {
        return getChanges().stream().filter(ch -> isDeleted(ch) && isDir(ch)).map(ch -> ch.getFile());
    }
}
