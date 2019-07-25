package com.batch.akbatch.dto;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotEmpty;

import org.hibernate.validator.constraints.Length;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@Entity
@Table(name = "Book")
public class Book {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private Long id;

	@NotEmpty
	@Length(max = 30)
	private String isbn;

	@NotEmpty
	@Length(max = 100)
	@Column(name = "BOOK_NAME")
	private String bookName;

	@NotEmpty
	@Length(max = 60)
	@Column(name = "AUTHOR_NAME")
	private String authorName;
	
	@Length(max = 50)
	private String genre;
	
	@Length(max = 600)
	@Column(name = "FILE_NAME")
	private String fileName;
}
