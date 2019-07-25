package com.batch.akbatch.util;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MakeFtpConnection {
	private String host;
	private String user;
	private String password;
	int port;
	private ChannelSftp sftpChannel;
	Session session;

	public void  openConnection() throws JSchException {
		JSch jsch = new JSch();

		session = jsch.getSession(user, host, port);
		session.setConfig("StrictHostKeyChecking", "no");
		session.setPassword(password);
		session.connect();

		Channel channel = session.openChannel("sftp");
		channel.connect();
		sftpChannel = (ChannelSftp) channel;
	}

	public void closeConnection()  {
		sftpChannel.exit();
		session.disconnect();
	}
}
