/**
 * 
 */
package org.sb.jgdrive;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.api.client.util.Key;
import com.google.api.services.drive.model.File;

/**
 * @author sam
 *
 */
public class RemoteIndex
{
    @Key
    private long lastSyncTimeEpochMillis;
    
    @Key
    private long lastRevisionId;
    
    @Key
    private HashMap<String, FileEntry> entryById = new HashMap<>();
    
    private final HashMap<String, Path> dirIdPathMapCache = new HashMap<>();
    
    public RemoteIndex(long lastRevisionId, Stream<File> files)
    {
        setLastSyncTime();
        this.lastRevisionId = lastRevisionId;
        files.forEach(f -> add(f));
    }

    public RemoteIndex()
    {
        
    }
    
    public static class FileEntry
    {
        @Key
        private String title;
        
        @Key
        private String parentId;
        
        public FileEntry()
        {
            
        }

        public FileEntry(File file)
        {
            this.title = file.getTitle();
            this.parentId = file.getParents().stream().findFirst().map(pr -> pr.getId()).orElse("root");
        }

        public FileEntry(String title, String parentId)
        {
            this.title = title;
            this.parentId = parentId;
        }
        
        private Path getLocalPath(Map<String, FileEntry> entryById, Map<String, Path> idPathMap)
        {
            Path parPath = idPathMap.get(parentId);
            if(parPath == null)
            {
                parPath = Optional.ofNullable(entryById.get(parentId))
                                    .map(fe -> fe.getLocalPath(entryById, idPathMap)).orElse(Paths.get(""));
                idPathMap.put(parentId, parPath);
            }
            return parPath.resolve(title); 
        }

        @Override
        public String toString()
        {
            return "FileEntry [title=" + title + ", parentId=" + parentId + "]";
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((parentId == null) ? 0 : parentId.hashCode());
            result = prime * result + ((title == null) ? 0 : title.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            FileEntry other = (FileEntry) obj;
            if (parentId == null)
            {
                if (other.parentId != null)
                    return false;
            }
            else if (!parentId.equals(other.parentId))
                return false;
            if (title == null)
            {
                if (other.title != null)
                    return false;
            }
            else if (!title.equals(other.title))
                return false;
            return true;
        }
    }
    
    public void add(Stream<File> files)
    {
        if(files.map(f -> add(f)).reduce(false, (a, e) -> a | e))
            dirIdPathMapCache.clear();
    }
    
    private boolean add(File file)
    {
        FileEntry fe = new FileEntry(file);
        return !fe.equals(entryById.put(file.getId(), fe));
    }
    
    void add(String fileId, String title, String parentId)
    {
        FileEntry fe = new FileEntry(title, parentId);
        entryById.put(fileId, fe);
    }

    public void remove(Stream<String> fileIds)
    {
        if(fileIds.map(f -> remove(f)).reduce(false, (a, e) -> a | e))
            dirIdPathMapCache.clear();
    }
    
    private boolean remove(String fileId)
    {
        return entryById.remove(fileId) != null;
    }
    
    public Map<Path, String> getFileId(Set<Path> localPath)
    {
        return entryById.entrySet().stream().filter(e -> localPath.contains(e.getValue().getLocalPath(entryById, dirIdPathMapCache)))
                                        .collect(Collectors.toMap(e -> e.getValue().getLocalPath(entryById, dirIdPathMapCache), e -> e.getKey()));
    }

    public Optional<Path> getLocalPath(String fileId)
    {
        return Optional.ofNullable(entryById.get(fileId)).map(fe -> fe.getLocalPath(entryById, dirIdPathMapCache));
    }

    public FileTime getLastSyncTime()
    {
        return FileTime.from(lastSyncTimeEpochMillis, TimeUnit.MILLISECONDS);
    }

    public void setLastSyncTime()
    {
        this.lastSyncTimeEpochMillis = System.currentTimeMillis();
    }

    public long getLastRevisionId()
    {
        return lastRevisionId;
    }

    public void setLastRevisionId(Long lastRevisionId)
    {
        this.lastRevisionId = lastRevisionId;
    }

    @Override
    public String toString()
    {
        return "RemoteIndex [lastSyncTimeEpochNanos=" + lastSyncTimeEpochMillis + ", lastRevisionId=" + lastRevisionId + ", entryById="
                + entryById + "]";
    }
    
    public int size()
    {
        return entryById.size();
    }
    
    public Stream<String> fileIds()
    {
        return entryById.keySet().stream();
    }

    public Stream<Path> localPaths()
    {
        return entryById.values().stream().map(fe -> fe.getLocalPath(entryById, dirIdPathMapCache));
    }
}
