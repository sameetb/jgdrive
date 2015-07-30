package org.sb.jgdrive;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.api.services.drive.model.File;

public class Info implements Cmd
{

    @Override
    public void exec(Driver driver, List<String> opts) throws IOException, IllegalStateException
    {
        Map<String, String> nvpFlags = Cmd.nvpFlags(opts.stream());
        String path = nvpFlags.get("path");
        String id = nvpFlags.get("id");
        if(path != null)
            printPath(driver, opts, path);
        else if(id != null)
            printFile(driver, id);
        else
            throw new IllegalArgumentException("Please specify a 'path/id' option for the command 'info'");
    }

    private void printPath(Driver driver, List<String> opts, String path) throws IOException
    {
        Map<Path, String> pathFileIdMap = driver.getRemoteIndex().getFileId(Stream.of(Paths.get(path)));
        if(Cmd.booleanFlags(opts.stream()).contains("full"))
        {
            Map<String, File> fileMap = driver.getFiles(pathFileIdMap.values().stream());
            System.out.println(pathFileIdMap.entrySet().stream()
                        .collect(Collectors.<Entry<Path, String>, Path, File>toMap(e -> e.getKey(), e -> fileMap.get(e.getValue()))));
        }
        else
            System.out.println(pathFileIdMap);
    }

    private void printFile(Driver driver, String id) throws IOException
    {
        Map<String, File> fileMap = driver.getFiles(Stream.of(id));
        System.out.println(fileMap.get(id));
    }
    
}
