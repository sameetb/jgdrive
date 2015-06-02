package org.sb.jgdrive;

import java.io.IOException;
import java.util.List;

public interface Cmd
{
    void exec(Driver driver, List<String> opts) throws IOException, IllegalStateException;
}
