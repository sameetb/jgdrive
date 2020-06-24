/**
 * 
 */
package org.sb.jgdrive;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * @author sameetb
 */
public class Index implements Cmd
{
    private static final Logger log = Logger.getLogger(Index.class.getPackage().getName());
    
    /* (non-Javadoc)
     * @see org.sb.jgdrive.Cmd#exec(org.sb.jgdrive.Driver, java.util.List)
     */
    @Override
    public void exec(Driver driver, List<String> opts) throws IOException, IllegalStateException
    {
        Map<String, String> nvpFlags = Cmd.nvpFlags(opts.stream());
        String op = nvpFlags.get("operation");
        String path = nvpFlags.get("path");
        switch(op)
        {
            case "removePath" :
                log.info("Removing path " + path);
                final Supplier<Stream<Path>> paths = () -> Stream.of(path).map(p -> Paths.get(path));
                log.info("Found:" + driver.getRemoteIndex().getFileId(paths.get()));
                log.info("Result:" + driver.getRemoteIndex().removePaths(paths.get()));
                driver.saveRemoteIndex();
                break;
        }
    }

}
