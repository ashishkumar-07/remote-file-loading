package com.batch.akbatch.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@Setter
@NoArgsConstructor
@Getter
public class BookDto {

	private String isbn;

	private String bookName;

	private String authorName;

	private String genre;
}
