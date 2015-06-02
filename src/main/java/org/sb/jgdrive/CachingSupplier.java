package org.sb.jgdrive;

import java.util.Optional;
import java.util.function.Supplier;

public class CachingSupplier<T> implements Supplier<T>
{
    private Optional<T> val;
    private Supplier<T> sup;
    
    public static <T> Supplier<T> wrap(Supplier<T> sup)
    {
        return new CachingSupplier<T>(sup);
    }
    
    private CachingSupplier(Supplier<T> sup)
    {
        this.sup = sup;
    }

    @Override
    public T get()
    {
        Optional<T> tmp = val;
        if(tmp == null)
        synchronized (this)
        {
            tmp = val;
            if(tmp == null)
            {
                val = tmp = Optional.ofNullable(sup.get());
                sup = null;
            }
        }
        return tmp.orElse(null);
    }
}
