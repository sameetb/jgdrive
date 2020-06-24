package org.sb.jgdrive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.xml.bind.DatatypeConverter;

import com.google.api.services.drive.model.File;

public class Pull implements Cmd
{
    private static final Logger log = Logger.getLogger(Pull.class.getPackage().getName());

    public void exec(final Driver driver, final List<String> opts) throws IOException, IllegalStateException
    {
        RemoteIndex ri = driver.getRemoteIndex();
        RemoteChanges remChanges = driver.getRemoteChanges(ri.getLastRevisionId(), Optional.of(ri.getLastSyncTime()));
        if(remChanges.getLargestChangeId() == ri.getLastRevisionId())
        {
            log.info("Already up to date at revision: " + ri.getLastRevisionId() + ". Nothing to pull.");
            return;
        }
        log.info("Pulling to revision: " + remChanges.getLargestChangeId());
        Path home = driver.getHome();
        LocalChanges lc = new LocalChanges(driver);
        ri.setLastSyncTime();
        
        Set<String> changedIds = remChanges.getChanges().stream().filter(ch -> ch.getFile() != null).map(ch -> ch.getFile().getId()).collect(Collectors.toSet());
        
        final Set<Entry<Path, String>> dels = !opts.contains("ignore-deletes") ?  lc.getDeletedPaths().entrySet() : Collections.emptySet();
        
		Set<Path> conflicts = Stream.of(lc.getModifiedFiles().entrySet(), 
                                                    dels, lc.getMovedFilesFromTo().keySet())
                               .flatMap(es -> es.stream())
                               .filter(e -> changedIds.contains(e.getValue()))
                               .map(e -> e.getKey()).collect(Collectors.toSet());
        
        
        /*Set<Path> conflicts = remChanges.getChanges().stream().map(ch -> ri.getLocalPath(ch.getFile().getId()))
                .filter(op -> op.map(p -> localChanges.contains(p)).orElse(false)).map(op -> op.get())*/
                
        if (!conflicts.isEmpty())
            throw new IllegalStateException("Found following local changes that conflict with remote changes. "
                    + "Please remove these locally modified files: " + conflicts
                    + "from the working directory (files might have moved, use 'status --local-only' to detect moves) "
                    + "and then attempt a pull passing the '--ignore-deletes' option ");
        
        List<Path> deletePaths = ri.remove(Stream.concat(remChanges.getDeletedFiles(), remChanges.getDeletedDirs()).map(f -> f.getId()));
        
        Map<File, Path> newFilePathMap = ri.add(Stream.concat(remChanges.getModifiedDirs(), remChanges.getModifiedFiles()));
        if(!opts.contains("ignore-new") && !Collections.disjoint(newFilePathMap.values(), lc.getNewFiles()))
        {
            HashSet<Path> ints = new HashSet<Path>(newFilePathMap.values());
            ints.retainAll(lc.getNewFiles());
			throw new IllegalStateException("These local new files conflict with upstream changes: " + ints
					+ "Please move these files from the working directory "
					+ " OR attempt a pull passing the '--ignore-new' option ");

        }
            
        Stream<Entry<File, Path>> remModifiedFiles = driver.downloadFiles(remChanges.getModifiedFiles()
        												.map(f -> new SimpleImmutableEntry<>(f, newFilePathMap.get(f)))
        												.filter(sie -> sie.getValue() != null)
	                                                    .filter(sie -> isModified(sie.getKey().getMd5Checksum(), home.resolve(sie.getValue())))
	                                                    .filter(sie -> isIgnored(driver.getRemIgnores(), newFilePathMap.get(sie.getValue())))
	                                                    .map(sie -> sie.getKey()));
        
        log.fine("Updating directories ...");
        remChanges.getModifiedDirs().forEach(f -> Optional.ofNullable(newFilePathMap.get(f))
        							.filter(p -> isIgnored(driver.getRemIgnores(), p))
        							.map(Try.uncheckFunction(p -> createDir(home.resolve(p)))));
        
        log.fine("Updating files ...");
        remModifiedFiles.forEach(Try.uncheck(e -> moveFile(e.getValue(), 
                    home.resolve(Optional.ofNullable(newFilePathMap.get(e.getKey())).orElseThrow(
                            () -> new IllegalStateException("Remote file " + e.getKey() + " was not added to index."))))));

        log.fine("Deleting files ...");
        deletePaths.stream().forEach(Try.uncheck(p -> deletePath(home.resolve(p))));
        
        ri.setLastRevisionId(remChanges.getLargestChangeId());
        log.info("Updated to revision: " + ri.getLastRevisionId());
        driver.saveLocalChanges(lc.getModifiedFiles().keySet());
        driver.saveRemoteIndex();
    }
    
    private boolean isModified(String checkSum, Path path)
    {
        try
        {
            if(Files.notExists(path)) return true;
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
            boolean isMod = !checkSum.equalsIgnoreCase(DatatypeConverter.printHexBinary(md5.digest()));
            if(!isMod)
            	log.info("Exists local '" + path + "'");
			return isMod;
        }
        catch (NoSuchAlgorithmException | IOException e)
        {
            throw new RuntimeException("Failed to generate MD5 hash of " + path, e);
        }
    }

    private boolean isIgnored(Supplier<Pattern[]> pats, Path path)
    {
        if(path == null)
            return true;
            
    	boolean ign = Stream.of(pats.get()).map(Pattern::asPredicate).anyMatch(pred -> pred.test(path.toString()));
    	if(ign)
    		log.info("Ignoring remote '" + path + "'");
		return ign;
    }
    
    private boolean deletePath(Path p) throws IOException
    {
        log.info("Deleting local '" + p + "'");
        return Files.deleteIfExists(p);
    }

    private void moveFile(Path tmpPath, Path lp) throws IOException
    {
        log.info("Updating local '" + lp + "'");
        Files.move(tmpPath, lp, StandardCopyOption.REPLACE_EXISTING);
    }

    private boolean createDir(Path p) throws IOException
    {
        if(!Files.exists(p, LinkOption.NOFOLLOW_LINKS))
        {
            log.info("Adding local '" + p + "'");
            Files.createDirectories(p);
            return true;
        }
        return false;
    }
    
}
