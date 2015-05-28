package org.sb.jgdrive;

import java.io.IOException;

public interface Cmd
{
    void exec() throws IOException, IllegalStateException;
}
