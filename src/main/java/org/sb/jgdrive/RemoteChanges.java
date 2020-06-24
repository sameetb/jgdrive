package org.sb.jgdrive;

import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.api.services.drive.model.Change;
import com.google.api.services.drive.model.File;

public class RemoteChanges
{
	private static final Logger log = Logger.getLogger(RemoteChanges.class.getPackage().getName());
	
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
                .filter(ch -> !isDeleted(ch) && !isDir(ch) && !needsExport(ch.getFile()))
                .map(ch -> ch.getFile());
    }

    private boolean isDir(Change ch)
    {
        return ch.getFile() != null && ch.getFile().getMimeType().equals("application/vnd.google-apps.folder");
    }

    private boolean isDeleted(Change ch)
    {
        return ch.getDeleted() || ch.getFile() == null || ch.getFile().getLabels().getTrashed();
    }
    
    public Stream<File> getModifiedDirs()
    {
        return getChanges().stream().filter(ch -> !isDeleted(ch) && isDir(ch)).map(ch -> ch.getFile());
    }

    public Stream<File> getDeletedFiles()
    {
        return getChanges().stream().filter(ch -> isDeleted(ch) && !isDir(ch)).map(ch -> deletedFile(ch));
    }

    public Stream<File> getDeletedDirs()
    {
        return getChanges().stream().filter(ch -> isDeleted(ch) && isDir(ch)).map(ch -> deletedFile(ch));
    }

	private static File deletedFile(Change ch) 
	{
		return Optional.ofNullable(ch.getFile()).orElseGet(() -> new File().setId(ch.getFileId()));
	}
	
    static boolean needsExport(File f) {
    	boolean exp = (f != null && f.getMimeType().startsWith("application/vnd.google-apps."));
    	if(exp)
    		log.log(Level.WARNING, "Ignoring non-downloadable file " + f.getId() + ":" + f.getTitle());
		return exp;
	}	
}
