package com.arnavsaraf.bookstagramdataloader.author.repository;

import com.arnavsaraf.bookstagramdataloader.author.model.Author;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AuthorRepository extends CassandraRepository<Author,String> {
}
