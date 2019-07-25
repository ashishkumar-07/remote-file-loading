package com.batch.akbatch.util;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.springframework.core.io.FileSystemResource;

import com.jcraft.jsch.SftpException;

public class RemoteResource extends FileSystemResource{
	
	MakeFtpConnection ftpRemoteFileTemplate;

	public RemoteResource(String filePath, MakeFtpConnection ftpRemoteFileTemplate) {
		super(filePath);
		this.ftpRemoteFileTemplate=ftpRemoteFileTemplate;
	}
	
	/**
	 * This implementation returns whether the underlying file exists.
	 * @see java.io.File#exists()
	 */
	@Override
	public boolean exists() {
		return (super.getFilename() != null );		
	}

	/**
	 * This implementation checks whether the underlying file is marked as readable
	 * (and corresponds to an actual file with content, not to a directory).
	 * @see java.io.File#canRead()
	 * @see java.io.File#isDirectory()
	 */
	@Override
	public boolean isReadable() {
		return (super.getFilename()  != null);
	}
	
	/**
	 * This implementation opens a NIO file stream for the underlying file.
	 * @see java.io.FileInputStream
	 */
	@Override
	public InputStream getInputStream() throws IOException {
		try {
			return ftpRemoteFileTemplate.getSftpChannel().get(super.getPath());
			
		}
		catch (SftpException ex) {
			throw new FileNotFoundException(ex.getMessage());
		}
	}
}
