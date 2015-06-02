package org.sb.jgdrive;

import java.io.IOException;
import java.util.List;

public class Login implements Cmd
{
    public void exec(final Driver driver, List<String> opts) throws IOException, IllegalStateException
    {
        CredHelper credHelper = CredHelper.makeCredHelper(driver.getHome());
        credHelper.reauthorize();
    }
}
