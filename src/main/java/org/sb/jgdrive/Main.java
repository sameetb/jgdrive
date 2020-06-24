package org.sb.jgdrive;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
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
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class Main
{
    private static Supplier<Logger> log = CachingSupplier.wrap(() -> Logger.getLogger(Main.class.getPackage().getName()));
    private static final String[] cmds = {"clone", "pull", "push", "status", "reset", "login", "info", "index"};
    
    public static void main(String[] args)
    {
    	int exit = 0;
        try
        {
            exec(args);
        }
        catch(Exception io)
        {
            log.get().log(Level.SEVERE, "", io);
            exit = -1;
        }   
        LogManager.getLogManager().reset();
        System.exit(exit);
    }
    
    public static void exec(String[] args) throws Exception
    {
        final LinkedList<Entry<String, List<String>>> cmds = parseArgs(args);
        final List<String> flags = cmds.removeFirst().getValue();

        Set<String> boolFlags = Cmd.booleanFlags(flags.stream());
        
        if(boolFlags.contains("help"))
        {
            help();
            return;
        }
        initLogging(boolFlags.contains("debug"), boolFlags.contains("debug-all"));
        
        final boolean simulation = boolFlags.contains("simulation");
        
        if(simulation) log.get().info("Running in simulation mode, no changes will be made on remote drive.");
        
        Map<String, String>  nvpFlags = Cmd.nvpFlags(flags.stream());
        final Path home = Optional.ofNullable(nvpFlags.get("home")).map(p -> Paths.get(p).toAbsolutePath())
                                                        .orElseGet(() -> Paths.get(".").toAbsolutePath().getParent());
        
        final Try<Driver, IOException> driver = cmds.stream().filter(cmd -> "clone".equals(cmd.getKey())).findAny().map(
                            Try.wrap(cmd ->
                                    {
                                        Clone clone = new Clone(home, simulation);
                                        clone.exec(cmd.getValue());
                                        return clone.getDriver();
                                    }, IOException.class))
                            .orElseGet(Try.wrap(() -> new Driver(home, simulation), IOException.class));

        cmds.stream().filter(cmd -> !"clone".equals(cmd.getKey()))
            .map(cmd -> makeCmd(cmd.getKey()).<Entry<Try<Cmd, Exception>, List<String>>>map(tc -> pair(tc, cmd.getValue())))
            .filter(otc -> otc.isPresent())
            .map(otc -> otc.get())
            .map(tsie -> tsie.getKey().flatMap(cmd -> driver.flatMap(dr -> cmd.exec2(dr, tsie.getValue()))))
            .forEach(Try.uncheck(tsie -> tsie.orElseThrow()));

    }
    
    private static <K, V> Entry<K, V> pair(K k, V v)
    {
        return new SimpleImmutableEntry<>(k, v);
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
                "\t--debug-all\t everything logs  at 'fine' verbosity level to the $HOME/.jgdrive.log file",
                "",
                "If no commands are specified, it defaults to 'pull push'.",
                "",
                "supported commands:"
                };
            Stream.concat(
                    Stream.of(msgs),
                    Stream.of(cmds).flatMap(c -> makeCmd(c).map(Try.uncheckFunction(cmd -> cmd.orElseThrow().help(c)))
                                                            .orElseGet(() -> Clone.help(c)).stream())
                                   .map(s -> "\t" + s))
                .forEach(s -> System.out.println(s));
    }

    private static Optional<Try<Cmd, Exception>> makeCmd(String cmd)
    {
        String clsNm = Cmd.class.getPackage().getName() + "." + cmd.substring(0, 1).toUpperCase() + cmd.substring(1);
        try
        {
            @SuppressWarnings("rawtypes")
            Class cmdCls = Class.forName(clsNm);
            if(Cmd.class.isAssignableFrom(cmdCls))
            {
                return Optional.of(Try.success((Cmd)cmdCls.newInstance()));
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
            return Optional.of(Try.failure(e));
        }
    }
    
    private static void initLogging(boolean debug, boolean debugAll) throws IOException
    {
        InputStream props;
        if(System.getProperty("java.util.logging.config.file") == null &&
                (props = Main.class.getClassLoader().getResourceAsStream("logging.properties")) != null)
        {
            if(debugAll)
            {
                Properties p = new Properties();
                p.load(props);
                p.setProperty(".level", Level.FINE.getName());
                p.setProperty("handlers", Optional.ofNullable(p.getProperty("handlers")).map(h -> h + ",").orElse("")
                                                                        + "java.util.logging.FileHandler");
                p.setProperty("java.util.logging.FileHandler.formatter", "java.util.logging.SimpleFormatter");
                p.setProperty("java.util.logging.FileHandler.pattern", "%h/.jgdrive.log");
                p.setProperty("java.util.logging.FileHandler.append", "true");
                p.setProperty("java.util.logging.ConsoleHandler.level", Level.INFO.getName());
                props.close();
                StringWriter sw = new StringWriter();
                p.store(sw, "");
                props = new ByteArrayInputStream(sw.toString().getBytes());
            }
            LogManager.getLogManager().readConfiguration(props);
            if(debug | debugAll)
                log.get().setLevel(Level.FINE);
            props.close();
        }
        else
            System.err.println("Java logging config system property is set or could not find 'logging.properties' on classpath");
    }
    
    private static LinkedList<Entry<String, List<String>>> parseArgs(String[] args)
    {
        LinkedList<Entry<String, List<String>>> cmds = new LinkedList<>();
        cmds.add(pair("", new ArrayList<>()));
        Stream.of(args).filter(arg -> arg.length() > 0)
        .forEach(arg -> 
        {
            Entry<String, List<String>> e = cmds.getLast();
            if(arg.startsWith("--")) e.getValue().add(arg.substring(2));
            else cmds.add(pair(arg.toLowerCase(), new ArrayList<>()));
        });
        if(cmds.size() < 2)
        {
            cmds.add(pair("pull", Collections.emptyList()));
            cmds.add(pair("push", Collections.emptyList()));
        }
        return cmds;
    }
}
