package org.sb.jgdrive;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * 
 * @author sam
 *
 * @param <T>
 * @param <E>
 */
public final class Try<T, E extends Exception>
{
    @FunctionalInterface
    public interface SupplierEx<T, E extends Exception>
    {
        T get() throws E;
    }
    
    @FunctionalInterface
    public interface ConsumerEx<T>
    {
        void accept(T t) throws Exception;
    }

    @FunctionalInterface
    public interface FunctionEx<T, R, E extends Exception>
    {
        R apply(T t) throws E;
    }
    
    /**
     * If non-null, the value; if null, indicates no value is present
     */
    private final T value;

    private final E ex;

    public static <T, E extends Exception> Try<T, E> failure(E ex)
    {
        return new Try<>(ex);
    }

    public static <T, E extends Exception> Try<T, E> failure(E ex, Class<T> tClass)
    {
        return new Try<>(ex);
    }

    public static <T, E extends Exception> Try<T, E> success(T t, Class<E> exClass)
    {
        return new Try<T, E>(t);
    }
    
    /**
     * Constructs an instance with the value present.
     *
     * @param value
     *            the non-null value to be present
     * @throws NullPointerException
     *             if value is null
     */
    private Try(T value)
    {
        this.value = value;
        this.ex = null;
    }

    private Try(E ex)
    {
        this.value = null;
        this.ex = Objects.requireNonNull(ex);
    }

    /**
     * Returns an {@code Try} with the specified present non-null value.
     *
     * @param <T>
     *            the class of the value
     * @param value
     *            the value to be present, which must be non-null
     * @return an {@code Try} with the value present
     * @throws NullPointerException
     *             if value is null
     */
    public static <T, E extends Exception> Try<T, E> success(T value)
    {
        return new Try<>(value);
    }

    
    /**
     * If a value is present in this {@code Try}, returns the value,
     * otherwise throws {@code IllegalStateException}.
     *
     * @return the non-null value held by this {@code Try}
     * @throws IllegalStateException
     *             if there is no value present
     *
     * @see Try#isSuccessful()
     */
    public T get()
    {
        if(ex == null) return value;
        throw new IllegalStateException(ex);
    }
    
    /**
     * @throws IllegalStateException if value is present
     * @see Try#isSuccessful()
     */
    public E getFailure()
    {
        if(ex != null) return ex;
        throw new IllegalStateException("The value is present.");
    }

    /**
     * Return {@code true} if there is a value present, otherwise {@code false}.
     *
     * @return {@code true} if there is a value present, otherwise {@code false}
     */
    public boolean isSuccessful()
    {
        return ex == null;
    }

    /**
     * If a value is present, invoke the specified consumer with the value,
     * otherwise do nothing.
     *
     * @param consumer
     *            block to be executed if a value is present
     * @throws NullPointerException
     *             if value is present and {@code consumer} is null
     */
    public void ifSuccessful(Consumer<? super T> consumer)
    {
        if (ex == null)
            consumer.accept(value);
    }

    public Optional<T> toOptional()
    {
        if (!isSuccessful())
            return Optional.empty();
        return Optional.ofNullable(value);
    }

    public Optional<Try<T, E>> filter(Predicate<? super T> predicate)
    {
        if (!isSuccessful())
            return Optional.empty();
        return predicate.test(value) ? Optional.ofNullable(this) : Optional.empty();
    }

    public <U> Try<U, E> map(Function<? super T, ? extends U> mapper)
    {
        Objects.requireNonNull(mapper);
        if (!isSuccessful())
            return new Try<U, E>(ex);
        else
        {
            return new Try<U, E>(mapper.apply(value));
        }
    }

    public <U> Try<U, E> flatMap(Function<? super T, ? extends Try<? extends U, ? extends E>> mapper)
    {
        Objects.requireNonNull(mapper);
        if (!isSuccessful())
            return new Try<U, E>(ex);
        else
        {
            Try<? extends U, ? extends E> fm = Objects.requireNonNull(mapper.apply(value));
            if(fm.isSuccessful()) return new Try<U, E>(fm.get());
            return new Try<U, E>(fm.getFailure());
        }
    }

    /**
     * Return the value if present, otherwise return {@code other}.
     *
     * @param other
     *            the value to be returned if there is no value present, may be
     *            null
     * @return the value, if present, otherwise {@code other}
     */
    public T orElse(T other)
    {
        return ex == null ? value : other;
    }

    /**
     * Return the value if present, otherwise invoke {@code other} and return
     * the result of that invocation.
     *
     * @param other
     *            a {@code Supplier} whose result is returned if no value is
     *            present
     * @return the value if present otherwise the result of {@code other.get()}
     * @throws NullPointerException
     *             if value is not present and {@code other} is null
     */
    public T orElseGet(Supplier<? extends T> other)
    {
        return ex  == null ? value : other.get();
    }

    /**
     * Return the contained value, if present, otherwise throw an exception to
     * be created by the provided supplier.
     *
     * @apiNote A method reference to the exception constructor with an empty
     *          argument list can be used as the supplier. For example,
     *          {@code IllegalStateException::new}
     *
     * @param <X>
     *            Type of the exception to be thrown
     * @param exceptionSupplier
     *            The supplier which will return the exception to be thrown
     * @return the present value
     * @throws X
     *             if there is no value present
     * @throws NullPointerException
     *             if no value is present and {@code exceptionSupplier} is null
     */
    public T orElseThrow() throws E
    {
        if (ex == null)
        {
            return value;
        }
        else
        {
            throw ex;
        }
    }

    /**
     * Indicates whether some other object is "equal to" this Try. The other
     * object is considered equal if:
     * <ul>
     * <li>it is also an {@code Try} and;
     * <li>both instances have no value present or;
     * <li>the present values are "equal to" each other via {@code equals()}.
     * </ul>
     *
     * @param obj
     *            an object to be tested for equality
     * @return {code true} if the other object is "equal to" this object
     *         otherwise {@code false}
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }

        if (!(obj instanceof Try))
        {
            return false;
        }

        Try<?, ?> other = (Try<?, ?>) obj;
        return Objects.equals(value, other.value);
    }

    /**
     * Returns the hash code value of the present value, if any, or 0 (zero) if
     * no value is present.
     *
     * @return hash code value of the present value or 0 if no value is present
     */
    @Override
    public int hashCode()
    {
        return Objects.hashCode(value);
    }

    /**
     * Returns a non-empty string representation of this Try suitable for
     * debugging. The exact presentation format is unspecified and may vary
     * between implementations and versions.
     *
     * @implSpec If a value is present the result must include its string
     *           representation in the result. Empty and present Trys must be
     *           unambiguously differentiable.
     *
     * @return the string representation of this instance
     */
    @Override
    public String toString()
    {
        return ex == null ? String.format("Try.success[%s]", value) : "Try.failure[" + ex.getMessage() + "]";
    }
    
    public static <T> Supplier<Try<T, Exception>> wrap(SupplierEx<? extends T, Exception> sup)
    {
        return wrap(sup, Exception.class);
    }
    
    public static <T, E extends Exception> Supplier<Try<T, E>> wrap(SupplierEx<? extends T, ? extends E> sup, 
                                                                                                    Class<E> exClass)
    {
        Objects.requireNonNull(sup);
        return () ->
        {
            try
            {
                return new Try<>(sup.get());
            }
            catch(Exception e)
            {
                if(exClass.isInstance(e))
                    return new Try<>(exClass.cast(e));
                throw new TryWrappedException(e);
            }
        };
    }

    public static <T, R> Function<T, Try<R, Exception>> wrap(FunctionEx<? super T, ? extends R, Exception> func)
    {
        return wrap(func);
    }
    
    public static <T, R, E extends Exception> Function<T, Try<R, E>> wrap(FunctionEx<? super T, ? extends R, 
                                                                                   ? extends E> func, Class<E> exClass)
    {
        Objects.requireNonNull(func);
        return t ->
        {
            try
            {
                return new Try<>(func.apply(t));
            }
            catch(Exception e)
            {
                if(exClass.isInstance(e))
                    return new Try<>(exClass.cast(e));
                throw new TryWrappedException(e);
            }
        };
    }

    @SuppressWarnings("serial")
    public static class TryWrappedException extends RuntimeException
    {
        public TryWrappedException(Exception cause)
        {
            super(cause);
        }
    }


    public static <T> Consumer<T> uncheck(ConsumerEx<? super T> con)
    {
        Objects.requireNonNull(con);
        return t -> 
        {
            try
            {
                con.accept(t);
            }
            catch (Exception ex)
            {
                throwUnchecked(ex);
            }
        };
    }

    public static <T> Supplier<T> uncheck(SupplierEx<? extends T, ?> sup)
    {
        Objects.requireNonNull(sup);
        return () -> 
        {
            try
            {
               return sup.get();
            }
            catch (Exception ex)
            {
                throwUnchecked(ex);
                return null;
            }
        };
    }

    public static <T, R> Function<T, R> uncheckFunction(FunctionEx<? super T, ? extends R, ?> func)
    {
        Objects.requireNonNull(func);
        return t -> 
        {
            try
            {
               return func.apply(t);
            }
            catch (Exception ex)
            {
                throwUnchecked(ex);
                return null;
            }
        };
    }
    
    @SuppressWarnings ("unchecked")
    private static <E extends Throwable> void throwUnchecked(Exception exception) throws E
    {
        throw (E) exception;
    }
}
