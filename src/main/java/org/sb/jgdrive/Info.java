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
        if(path == null) throw new IllegalArgumentException("Please specify a 'path' option for the command 'info'");
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

}
