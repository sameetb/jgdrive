package org.sb.jgdrive;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.DatatypeConverter;

public class ChecksumInputStream extends FilterInputStream
{
    private String md5HexChecksum;
    private final MessageDigest md5;
    protected ChecksumInputStream(InputStream in)
    {
        super(in);
        try
        {
            md5 = MessageDigest.getInstance("MD5");
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
        if(r != -1) md5.update((byte)r);
        return r;
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        int i = super.read(b, off, len);
        if(i != -1) md5.update(b, off, i);
        return i;
    }
    
    @Override
    public synchronized void reset() throws IOException
    {
        super.reset();
        md5.reset();
    }

    @Override
    public void close() throws IOException
    {
        super.close();
        md5HexChecksum = DatatypeConverter.printHexBinary(md5.digest());
    }

    public String getMd5HexChecksum()
    {
        return md5HexChecksum;
    }
}