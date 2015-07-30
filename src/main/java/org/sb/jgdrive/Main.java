package org.sb.jgdrive;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class Main
{
    private static Supplier<Logger> log = CachingSupplier.wrap(() -> Logger.getLogger(Main.class.getPackage().getName()));
    private static final String[] cmds = {"clone", "pull", "push", "status", "reset", "login", "info"};
    
    public static void main(String[] args)
    {
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
        LogManager.getLogManager().reset();
        System.exit(-1);
    }
    
    public static void exec(String[] args) throws IOException, IORtException
    {
        final LinkedList<Entry<String, List<String>>> cmds = parseArgs(args);
        final List<String> flags = cmds.removeFirst().getValue();

        Set<String> boolFlags = Cmd.booleanFlags(flags.stream());
        
        if(boolFlags.contains("help"))
        {
            help();
            return;
        }
        initLogging(boolFlags.contains("debug"));
        
        final boolean simulation = boolFlags.contains("simulation");
        
        if(simulation) log.get().info("Running in simulation mode, no changes will be on remote drive.");
        
        Map<String, String>  nvpFlags = Cmd.nvpFlags(flags.stream());
        final Path home = Optional.ofNullable(nvpFlags.get("home")).map(p -> Paths.get(p).toAbsolutePath())
                                                        .orElseGet(() -> Paths.get(".").toAbsolutePath().getParent());
        
        final Driver driver = cmds.stream().filter(cmd -> "clone".equals(cmd.getKey())).findAny().map(cmd -> {
                                    try
                                    {
                                        Clone clone = new Clone(home, simulation);
                                        clone.exec(cmd.getValue());
                                        return clone.getDriver();
                                    }
                                    catch(IOException e)
                                    {
                                        throw new IORtException(e); 
                                    }
                                }).orElseGet(() -> new Driver(home, simulation));
        
        cmds.stream().filter(cmd -> !"clone".equals(cmd.getKey()))
            .map(cmd -> makeCmd(cmd.getKey()).<Entry<Cmd, List<String>>>map(co -> new SimpleImmutableEntry<>(co, cmd.getValue())))
            .forEach(ocmd -> ocmd.ifPresent( cmd -> {   
                                try
                                {
                                    cmd.getKey().exec(driver, cmd.getValue());
                                }
                                catch(IOException e)
                                {
                                    throw new IORtException(e); 
                                }}));
    }

    private static void help()
    {
        String[] msgs = {
                "jgdrive 0.0.1, Google drive file sychronizer.",
                "usage: jgdrive [--home=<dir>] [--simulation] [--help] [--debug] [<command> [--<arg>]*]...",
                "",
                "options:",
                "\t--home=<dir>\tthe directory where the drive files are (to be) checked out.",
                "\t\t\tif not specified, the current directory is assumed to be the home.",
                "\t--simulation\tDry run, no changes will be made on remote drive.",
                "\t--debug\t application logs  at 'fine' verbosity level",
                "",
                "If no commands are specified, it defaults to 'pull push'.",
                "",
                "supported commands:"
                };
            Stream.concat(
                    Stream.of(msgs),
                    Stream.of(cmds).flatMap(c -> makeCmd(c).<List<String>>map(cmd -> cmd.help(c))
                                                            .orElseGet(() -> Clone.help(c)).stream())
                                   .map(s -> "\t" + s))
                .forEach(s -> System.out.println(s));
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
            log.get().info("Unsupported command: " + cmd);
            return Optional.empty();
        }
        catch (InstantiationException | IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    private static void initLogging(boolean debug) throws IOException
    {
        InputStream props;
        if(System.getProperty("java.util.logging.config.file") == null &&
                (props = Main.class.getClassLoader().getResourceAsStream("logging.properties")) != null)
        {
            LogManager.getLogManager().readConfiguration(props);
            if(debug)
                log.get().setLevel(Level.FINE);
        }
        else
            System.err.println("Java logging config system property is set or could not find 'logging.properties' on classpath");
    }
    
    private static LinkedList<Entry<String, List<String>>> parseArgs(String[] args)
    {
        LinkedList<Entry<String, List<String>>> cmds = new LinkedList<>();
        cmds.add(new SimpleImmutableEntry<>("", new ArrayList<>()));
        Stream.of(args).filter(arg -> arg.length() > 0)
        .forEach(arg -> 
        {
            Entry<String, List<String>> e = cmds.getLast();
            if(arg.startsWith("--")) e.getValue().add(arg.substring(2));
            else cmds.add(new SimpleImmutableEntry<>(arg.toLowerCase(), new ArrayList<>()));
        });
        if(cmds.size() < 2)
        {
            cmds.add(new SimpleImmutableEntry<>("pull", Collections.emptyList()));
            cmds.add(new SimpleImmutableEntry<>("push", Collections.emptyList()));
        }
        return cmds;
    }
}
