package com.arnavsaraf.bookstagramdataloader.book.repository;

import com.arnavsaraf.bookstagramdataloader.book.model.Book;
import org.springframework.data.cassandra.repository.CassandraRepository;

public interface BookRepository extends CassandraRepository<Book, String> {
}
