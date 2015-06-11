package org.sb.jgdrive;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.DatatypeConverter;

public class ChecksumInputStream extends FilterInputStream
{
    private String mdHexChecksum;
    private final MessageDigest md;
    protected ChecksumInputStream(InputStream in)
    {
        this("MD5", in);
    }

    protected ChecksumInputStream(String algo, InputStream in)
    {
        super(in);
        try
        {
            md = MessageDigest.getInstance(algo);
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public int read() throws IOException
    {
        int r = super.read();
        if(r != -1) md.update((byte)r);
        return r;
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        int i = super.read(b, off, len);
        if(i != -1) md.update(b, off, i);
        return i;
    }
    
    @Override
    public synchronized void reset() throws IOException
    {
        super.reset();
        md.reset();
    }

    @Override
    public void close() throws IOException
    {
        super.close();
        if(mdHexChecksum == null) mdHexChecksum = DatatypeConverter.printHexBinary(md.digest()).toLowerCase();
    }

    public String getHexChecksum()
    {
        return mdHexChecksum;
    }
}