package com.batch.akbatch.util;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Configuration
@ConfigurationProperties("remote")
@NoArgsConstructor
@AllArgsConstructor
@Data
public class FtpProperties {
	public String host;
	public int port;
	public String username;
	public String password;
	public String filePattern;
	public String sourcePath;

}
