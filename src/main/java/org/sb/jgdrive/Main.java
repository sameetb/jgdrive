package org.sb.jgdrive;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class Main
{
    public static void main(String[] args)
    {
        Supplier<Logger> log = () -> Logger.getLogger(Main.class.getPackage().getName());
        try
        {
            exec(args);
        }
        catch(IORtException  io)
        {
            log.get().log(Level.SEVERE, "", io.getCause());
        }
        catch(Exception io)
        {
            log.get().log(Level.SEVERE, "", io);
        }
        System.exit(-1);
    }
    
    public static void exec(String[] args) throws IOException
    {
        final Supplier<Stream<String>> sargs = () -> Stream.of(args).filter(arg -> arg.length() > 0).map(arg -> arg.toLowerCase()).distinct();
        final Supplier<Stream<String>> cmds = () -> sargs.get().filter(arg -> !arg.startsWith("-"));
        final Supplier<Stream<String>> flags = () -> sargs.get().filter(arg -> arg.startsWith("-")).map(arg -> arg.substring(1));

        initLogging(booleanFlag(flags, "debug"));
        
        if(booleanFlag(flags, "help"))
        {
            help();
            return;
        }
        
        final boolean simulation = booleanFlag(flags, "simulation");
        
        final Path home = nvpFlag(flags, "home").map(p -> Paths.get(p).toAbsolutePath())
                                                        .orElseGet(() -> Paths.get(".").toAbsolutePath().getParent());
        
        final Driver driver = cmds.get().filter(cmd -> "clone".equals(cmd)).findAny().map(cmd -> {
                                    try
                                    {
                                        Clone clone = new Clone(home, simulation);
                                        clone.exec(Collections.emptyList());
                                        return clone.getDriver();
                                    }
                                    catch(IOException e)
                                    {
                                        throw new IORtException(e); 
                                    }
                                }).orElseGet(() -> new Driver(home, simulation));
        //cmds.
        
        cmds.get().filter(cmd -> !"clone".equals(cmd)).map(cmd -> makeCmd(cmd))
            .forEach(ocmd -> ocmd.ifPresent( cmd -> {   
                                try
                                {
                                    cmd.exec(driver, Collections.emptyList());
                                }
                                catch(IOException e)
                                {
                                    throw new IORtException(e); 
                                }}));
    }

    private static void help()
    {
        System.out.println("Not implemented yet!");
        
    }

    private static Boolean booleanFlag(final Supplier<Stream<String>> flags, String flagName)
    {
        return flags.get().filter(flag -> flag.equals(flagName)).findFirst().map(flag -> true).orElse(false);
    }

    private static Optional<String> nvpFlag(final Supplier<Stream<String>> flags, String flagName)
    {
        return flags.get().filter(flag -> flag.startsWith(flagName + "=")).findFirst()
                        .map(flag -> flag.replace(flagName + "=", ""));
    }

    private static Optional<Cmd> makeCmd(String cmd)
    {
        String clsNm = Cmd.class.getPackage().getName() + "." + cmd.substring(0, 1).toUpperCase() + cmd.substring(1);
        try
        {
            Class cmdCls = Class.forName(clsNm);
            if(Cmd.class.isAssignableFrom(cmdCls))
            {
                return Optional.of((Cmd)cmdCls.newInstance());
            }
            return Optional.empty();
        }
        catch (ClassNotFoundException e)
        {
            System.err.println("Unsupported command: " + cmd);
            return Optional.empty();
        }
        catch (InstantiationException | IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    private static void initLogging(boolean debug) throws IOException
    {
        InputStream props = Main.class.getClassLoader().getResourceAsStream("logging.properties");
        if(props != null)
        {
            LogManager.getLogManager().readConfiguration(props);
            if(debug)
                Optional.ofNullable(LogManager.getLogManager()
                            .getLogger(Main.class.getPackage().getName())).ifPresent(log -> log.setLevel(Level.FINE));
        }
        else
            System.err.println("Could not find 'logging.properties' in classpath");
    }
}
