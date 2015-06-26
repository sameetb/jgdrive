/**
 * 
 */
package org.sb.jgdrive;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
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
    private static final Logger log = Logger.getLogger(Pull.class.getPackage().getName());
    
    @Key
    private long lastSyncTimeEpochMillis;
    
    @Key
    private long lastRevisionId;
    
    //@Key
    //private HashMap<String, FileEntry> entryById = new HashMap<>();
    
    //private final HashMap<String, Path> dirIdPathMapCache = new HashMap<>();
    
    @Key
    private CachingSupplier<Node> tree;

    public RemoteIndex(long lastSyncTimeEpochMillis, long lastRevisionId, Map<String, FileEntry> entryById)
    {
        this.lastSyncTimeEpochMillis = lastSyncTimeEpochMillis;
        this.lastRevisionId = lastRevisionId;
        tree = CachingSupplier.wrap2(() -> new TreeBuilder(entryById).getRoot());
    }
    
    public RemoteIndex(long lastRevisionId, String rootFolderId)
    {
        setLastSyncTime();
        this.lastRevisionId = lastRevisionId;
        tree = CachingSupplier.wrap2(() -> new Node(rootFolderId, rootFileEntry()));
    }

    private static FileEntry rootFileEntry()
    {
        return new FileEntry("", null);
    }

    public RemoteIndex()
    {
        //json deserialization constructor
    }
    
    public static class FileEntry
    {
        @Key("t")
        private String title;
        
        //@Key("p")
        private String parentId;
        
        public FileEntry()
        {
            
        }

        public FileEntry(File file)
        {
            this.title = file.getTitle();
            this.parentId = parentId(file);
        }

        public FileEntry(String title, String parentId)
        {
            this.title = title;
            this.parentId = parentId;
        }
        
        /*private Path getLocalPath(Map<String, FileEntry> entryById, Map<String, Path> idPathMap)
        {
            Path parPath = idPathMap.get(parentId);
            if(parPath == null)
            {
                parPath = Optional.ofNullable(entryById.get(parentId))
                                    .map(fe -> fe.getLocalPath(entryById, idPathMap)).orElse(Paths.get(""));
                idPathMap.put(parentId, parPath);
            }
            return parPath.resolve(title); 
        }*/

        @Override
        public String toString()
        {
            return "FileEntry [title=" + title + /*", parentId=" + parentId +*/ "]";
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            //result = prime * result + ((parentId == null) ? 0 : parentId.hashCode());
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
            /*if (parentId == null)
            {
                if (other.parentId != null)
                    return false;
            }
            else if (!parentId.equals(other.parentId))
                return false;*/
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
    
    public Map<File, Path> add(Stream<File> files)
    {
        /*if(files.map(f -> add(f)).reduce(false, (a, e) -> a | e))
            dirIdPathMapCache.clear();*/
        
        HashMap<File, Path> filePathMap = new HashMap<File, Path>();
        
        Map<String, File> addedFiles = files.collect(Collectors.toMap(f -> f.getId(), f -> f));
        Node t = tree.get();
        
        Set<String> searchIds = addedFiles.values().stream().flatMap(f -> Stream.of(f.getId(), parentId(f))).collect(Collectors.toSet());
        
        Map<String, Entry<Node, Path>>  searchRes 
                = t.find(Paths.get(""), searchIds).collect(Collectors.toMap(eoe -> eoe.getKey(), eoe -> eoe.getValue()));

        Stream<Entry<File, Entry<Node, Path>>> existingFiles = 
                addedFiles.entrySet().stream().filter(e -> searchRes.containsKey(e.getKey()))
                            .map(e -> new SimpleImmutableEntry<File, Entry<Node, Path>>(e.getValue(), searchRes.get(e.getKey())));

        Stream<File> newFiles = 
                addedFiles.entrySet().stream().filter(e -> !searchRes.containsKey(e.getKey())).map(e -> e.getValue());

        Map<String, Entry<Node, Path>> existingDirs = 
                searchRes.entrySet().stream().filter(e -> !addedFiles.containsKey(e.getKey()))
                            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

        HashMap<String, Node> remNodes = new HashMap<>();
        newFiles.forEach(f -> {
                    Node node = remNodes.remove(f.getId());
                    if(node == null) 
                        node = new Node(f.getId(), new FileEntry(f));
                    else 
                        node.entry = new FileEntry(f);
                    String parId = parentId(f);
                    Entry<Node, Path> parNode = existingDirs.get(parId);
                    if(parNode != null)
                    {
                        parNode.getKey().add(node);
                        node.nodes(parNode.getValue()).forEach(e -> {
                                                File child = addedFiles.get(e.getKey().id);
                                                filePathMap.put(child, e.getValue());
                                                if(isDir(child))
                                                    existingDirs.put(child.getId(), 
                                                            new SimpleImmutableEntry<>(e.getKey(), e.getValue()));
                        });
                    }
                    else
                    {
                        Optional<Node> parNod = 
                        remNodes.values().stream().map(n -> n.find(Paths.get(""), parId))
                                    .filter(os -> os.isPresent()).map(os -> os.get().getKey()).findFirst();
                        if(!parNod.isPresent()) 
                        {
                            parNod = Optional.of(new Node(parId, new FileEntry("", null)));
                            remNodes.put(parId, parNod.get());
                        }
                        parNod.get().add(node);
                    }
                });
         if(!remNodes.isEmpty())
            throw new RuntimeException("Could not resolve hierarchy for " + remNodes);

         existingFiles.forEach(e -> {
             Node match = e.getValue().getKey();
             File file = e.getKey();
             FileEntry fe = new FileEntry(file);
             Path matchPath = e.getValue().getValue();
             String parId = parentId(file);
             t.find(matchPath.getParent())
                 .filter(par -> !par.id.equals(parId))
                 .ifPresent(par -> 
                     {
                         par.remove(match);
                         existingDirs.get(parId).getKey().add(match);
                     });
             match.entry = fe;
             filePathMap.put(file, matchPath);
         });
     
        return filePathMap;
    }
    
    private boolean isDir(File f)
    {
        return f.getMimeType().equals(Driver.MIME_TYPE_DIR);
    }    

    /*private boolean add(File file)
    {
        FileEntry fe = new FileEntry(file);
        //return !fe.equals(entryById.put(file.getId(), fe));
        Node t = tree.get();
        Optional<Entry<Node, Path>> curr = t.find(Paths.get(""), file.getId());
        if(curr.isPresent())
        {
            Node match = curr.get().getKey();
            match.entry = fe;
            if(match.entry.parentId != fe.parentId)
            {
                t.find(curr.get().getValue().getParent()).map(on -> on.remove(match));
                t.find(Paths.get(""), fe.parentId).map(e -> e.getKey().add(match));
            }
            return true;
        }
        else
            t.find(Paths.get(""), fe.parentId).map(e -> e.getKey().add(new Node(file.getId(), fe)));
        
        return false;
    }*/
    
    /*void add(String fileId, String title, String parentId)
    {
        FileEntry fe = new FileEntry(title, parentId);
        entryById.put(fileId, fe);
    }*/

    public List<Path> remove(Stream<String> fileIds)
    {
        /*if(fileIds.map(f -> remove(f)).reduce(false, (a, e) -> a | e))
            dirIdPathMapCache.clear();*/
        List<Path> paths = tree.get().find(Paths.get(""), fileIds.collect(Collectors.toSet()))
                .map(e -> e.getValue().getValue()).collect(Collectors.toList());
        removePaths(paths.stream());
        return paths;
    }

    public void removePaths(Stream<Path> paths)
    {
        Node t = tree.get();
        paths.map(p -> new SimpleImmutableEntry<>(t.find(p.getParent()), p.getFileName().toString()))
             .filter(sie -> sie.getKey().isPresent())
             .map(sie -> new SimpleImmutableEntry<>(sie.getKey().get(), sie.getValue()))
             .forEach(sie -> sie.getKey().remove(sie.getValue()));
    }
    
    
    /*private boolean remove(String fileId)
    {
        return entryById.remove(fileId) != null;
    }*/
    
    public Map<Path, String> getFileId(Stream<Path> localPath)
    {
        /*return entryById.entrySet().stream().filter(e -> localPath.contains(e.getValue().getLocalPath(entryById, dirIdPathMapCache)))
                                        .collect(Collectors.toMap(e -> e.getValue().getLocalPath(entryById, dirIdPathMapCache), e -> e.getKey()));*/
        return localPath.map(p -> new SimpleImmutableEntry<Path, Optional<String>>(p,  tree.get().find(p).map(n -> n.id)))
                        .filter(sie -> sie.getValue().isPresent())
                        .collect(Collectors.toMap(sie -> sie.getKey(), sie -> sie.getValue().get()));
    }

    public Optional<Path> getLocalPath(String fileId)
    {
        return getLocalPath(Collections.singleton(fileId)).findFirst().flatMap(oe -> oe.getValue());
        //return Optional.ofNullable(entryById.get(fileId)).map(fe -> fe.getLocalPath(entryById, dirIdPathMapCache));
    }

    public Stream<Entry<String, Optional<Path>>> getLocalPath(Set<String> fileIds)
    {
        return tree.get().find2(Paths.get(""), new HashSet<>(fileIds))
                    .map(e -> new SimpleImmutableEntry<>(e.getKey(), e.getValue().map(en -> en.getValue())));
        //return Optional.ofNullable(entryById.get(fileId)).map(fe -> fe.getLocalPath(entryById, dirIdPathMapCache));
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
        return "RemoteIndex [lastSyncTimeEpochNanos=" + lastSyncTimeEpochMillis + ", lastRevisionId=" + lastRevisionId + "]";
    }
    
    public int size()
    {
        //return entryById.size();
        return (int)tree.get().nodes().count();
    }
    
    public Stream<String> fileIds()
    {
        return tree.get().nodes().map(n -> n.id);
        //return entryById.keySet().stream();
    }

    public Stream<Entry<Path, String>> localPaths()
    {
        return tree.get().paths(Paths.get(""));
        
        //return entryById.values().stream().map(fe -> fe.getLocalPath(entryById, dirIdPathMapCache));
    }
    
    public static class Node
    {
        @Key("i")
        private String id;
        
        @Key("f")
        private FileEntry entry;
        
        @Key("es")
        private HashSet<Node> entries;
        
        public Node()
        {
            
        }
        
        Node(String id, FileEntry entry)
        {
            this.id = id;
            this.entry = entry;
        }

        Optional<Node> remove(String id)
        {
            Optional<Node> node = getEntries().flatMap(e -> e.stream().filter(ch -> ch.id.equals(id)).findFirst());
            node.ifPresent(n -> remove(n));
            return node;
        }

        boolean remove(Node match)
        {
            return getEntries().map(e -> e.remove(match)).orElse(false);
        }

        boolean add(Node me)
        {
            if(entries == null) entries = new HashSet<>();
            if(!entries.add(me))
                entries.stream().filter(ch -> ch.equals(me)).findFirst().ifPresent(n -> n.entry = me.entry);
            return true;
        }
        
        Optional<Node> find(Path path)
        {
            int count;
            if(path == null || (count = path.getNameCount()) == 0) return Optional.empty();
            Optional<Node> child = getEntries().flatMap(e -> 
                                e.stream().filter(ch -> ch.entry.title.equals(path.getName(0).toString())).findFirst());
            if(count == 1) return child;
            return child.flatMap(ch -> ch.find(path.subpath(1, count)));
        }

        /*Stream<Entry<Path, Optional<Node>>> find(Set<Path> paths)
        {
            int count = paths.getNameCount();
            if(count == 0) return Optional.empty();
            Optional<Node> child = getEntries().flatMap(e -> 
                                e.stream().filter(ch -> ch.entry.title.equals(paths.getName(0).toString())).findFirst());
            if(count == 1) return child;
            return child.flatMap(ch -> ch.find(paths.subpath(1, count)));
        }*/
        
        private Optional<HashSet<Node>> getEntries()
        {
            return Optional.ofNullable(entries);
        }
        
        Optional<Entry<Node, Path>> find(Path parent, String fileId)
        {
            /*Path curr = parent.resolve(entry.title);
            if(id.equals(fileId))
                return Optional.of(new SimpleImmutableEntry<>(this, curr));
            
            return getEntries().flatMap(e -> e.stream().map(ch -> ch.find(curr, fileId))
                                                .filter(oe -> oe.isPresent()).map(oe -> oe.get()).findFirst());*/
            
            /*for(Node ch : entries)
            {
                Optional<Entry<Node, Path>> match = ch.find(curr, fileId);
                if(match.isPresent()) return match;
            }
            return Optional.empty();*/
            return find(parent, Stream.of(fileId).collect(Collectors.toSet()))
                    .findFirst().map(oe -> oe.getValue());
        }

        Stream<Entry<String, Entry<Node, Path>>> find(Path parent, Set<String> fileIds)
        {
            Path curr = parent.resolve(entry.title);
            Stream<Entry<String, Entry<Node, Path>>> res = fileIds.remove(id) ? 
                Stream.of(new SimpleImmutableEntry<>(id, new SimpleImmutableEntry<>(this, curr))) : Stream.empty();
            
            if(fileIds.isEmpty() || entries == null) return res;
            
            return Stream.concat(res, entries.stream().flatMap(ch -> ch.find(curr, fileIds)));
        }
        
        Stream<Entry<String, Optional<Entry<Node, Path>>>> find2(Path parent, Set<String> fileIds)
        {
            return Stream.concat(find(parent, fileIds)
                                    .map(e -> new SimpleImmutableEntry<>(e.getKey(), Optional.of(e.getValue()))), 
                    fileIds.stream().map(f -> new SimpleImmutableEntry<>(id, Optional.empty())));
        }
        
        Stream<Entry<Path, String>> paths(Path parent)
        {
            Path curr = parent.resolve(entry.title);
            return Stream.concat(Stream.of(new SimpleImmutableEntry<>(curr, id)), 
                    getEntries().map(e -> e.stream().<Entry<Path, String>>flatMap(ch -> ch.paths(curr))).orElse(Stream.empty()));
        }

        Stream<Node> nodes()
        {
            return Stream.concat(Stream.of(this), 
                    getEntries().map(e -> e.stream().<Node>flatMap(ch -> ch.nodes())).orElse(Stream.empty()));
        }

        Stream<Entry<Node, Path>> nodes(Path parent)
        {
            Path curr = parent.resolve(entry.title);
            return Stream.concat(Stream.of(new SimpleImmutableEntry<>(this, curr)), 
                    getEntries().map(e -> e.stream().<Entry<Node, Path>>flatMap(ch -> ch.nodes(curr))).orElse(Stream.empty()));
        }
        
        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((id == null) ? 0 : id.hashCode());
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
            Node other = (Node) obj;
            if (id == null)
            {
                if (other.id != null)
                    return false;
            }
            else if (!id.equals(other.id))
                return false;
            return true;
        }

        @Override
        public String toString()
        {
            return "Node [id=" + id + ", entry=" + entry + "]";
        }
    }

    private static class TreeBuilder
    {
        private Node root;
        private final HashMap<String, Node> map = new HashMap<>();

        TreeBuilder(Map<String, FileEntry> entryById)
        {
            entryById.entrySet().stream().forEach(e -> addNode(entryById, e.getKey(), e.getValue()));
        }
        
        private Node addNode(Map<String, FileEntry> entryById, String id, FileEntry f)
        {
            Node me = get(id);
            if(me == null)
            {
                me = new Node(id, f);
                put(id, me);
                if(f.parentId != null)
                    Optional.ofNullable(get(f.parentId)).orElseGet(() -> 
                                        addNode(entryById, f.parentId, Optional.ofNullable(entryById.get(f.parentId))
                                                        .orElseGet(() -> rootFileEntry())))
                            .add(me); 
                else
                    setRoot(me);
            }
            return me;
        }

        private Node get(String id)
        {
            return map.get(id);
        }

        private void put(String id, Node me)
        {
            map.put(id, me);
        }

        private void setRoot(Node me)
        {
            root = me;
        }

        Node getRoot()
        {
            return root;
        }
    }
    
    public boolean exists(Path path)
    {
        boolean ex = tree.get().find(path).map(p -> true).orElse(false);
        //return localPaths().filter(p -> p.equals(path)).findFirst().map(p -> true).orElse(false);
        if(!ex) log.fine("'" + path + "' does not exist");
        return ex;
    }

    static String parentId(File file)
    {
        return file.getParents().stream().findFirst().map(pr -> pr.getId()).orElse("root");
    }
}
