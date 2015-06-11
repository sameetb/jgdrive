package org.sb.jgdrive;

import java.util.function.Supplier;

import com.google.api.client.util.Key;

public class CachingSupplier<T> implements Supplier<T>
{
    @Key
    private T val;
    private Supplier<T> sup;
    
    public CachingSupplier()
    {
    }

    public static <T> Supplier<T> wrap(Supplier<T> sup)
    {
        return wrap2(sup);
    }

    public static <T> CachingSupplier<T> wrap2(Supplier<T> sup)
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
        Supplier<T> tmp = sup;
        if(tmp != null)
        synchronized (this)
        {
            tmp = sup;
            if(tmp != null)
            {
                val = tmp.get();
                tmp = sup = null;
            }
        }
        return val;
    }
}
