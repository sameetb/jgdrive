package org.sb.jgdrive;

import java.io.IOException;

@SuppressWarnings("serial")
public class IORtException extends RuntimeException
{

    public IORtException(IOException cause)
    {
        super(cause);
    }

    public IORtException(String string)
    {
        super(string);
    }
}
