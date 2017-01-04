package org.sb.jgdrive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class Put implements Cmd
{
    private static final Logger log = Logger.getLogger(Put.class.getPackage().getName());

    public void exec(final Driver driver, final List<String> opts) throws IOException, IllegalStateException
    {   
        Map<String, String> nvpFlags = Cmd.nvpFlags(opts.stream());
        String parentId = nvpFlags.get("parentid");
        Optional<Path> path = oPath(nvpFlags.get("path"));
        path.ifPresent(Try.uncheck(p -> driver.insertFile(p.toAbsolutePath(), parentId)));
        readPathFile(nvpFlags.get("path-file")).forEach(Try.uncheck(p -> driver.insertFile(p, parentId)));
    }
    
    public List<String> help(String name)
    {
        return Collections.singletonList(name + "\t --parentId <parentId> (--path | --path-file) <local-file-path>");
    }
    
    private Stream<Path> readPathFile(String pathFile) throws IOException
    {
        final Optional<Path> oPath = oPath(pathFile);
        if(oPath.isPresent()) 
            return  Files.readAllLines(oPath.get()).stream().map(s -> oPath(s)).filter(op -> op.isPresent())
                                                                               .map(op -> op.get());
        return Stream.empty();
    }
    
    private Optional<Path> oPath(String pathNm)
    {
        return Optional.ofNullable(pathNm)
                    .map(p -> Paths.get(p)).filter(p -> Files.isRegularFile(p) && Files.isReadable(p));
    }
}
