package com.arnavsaraf.bookstagramdataloader;

import com.arnavsaraf.bookstagramdataloader.author.model.Author;
import com.arnavsaraf.bookstagramdataloader.author.repository.AuthorRepository;
import com.arnavsaraf.bookstagramdataloader.config.DataStaxAstraConfig;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraConfig.class)
public class BookstagramDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	@Value("${datadump.location.author}")
	private String authorDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;


	public static void main(String[] args) {
		SpringApplication.run(BookstagramDataLoaderApplication.class, args);
	}

	private void initAuthors(){
		System.out.println("Starting data load");
		Path path = Paths.get(authorDumpLocation);
		try(Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject json = new JSONObject(jsonString);
					Author author = new Author(json.optString("key").replace("/authors/",""),
							json.optString("name"),
							json.optString("personal_name"));

					System.out.println("Saving data in cassandra "  + author.getName());
					authorRepository.save(author);
				} catch (JSONException e) {
					e.printStackTrace();
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private void initWorks(){}
	@PostConstruct
	public void start(){
		System.out.println(authorDumpLocation);
		initAuthors();
		Author author = new Author("1","Name","PersonalName");
//		authorRepository.save(author);
		System.out.println("Finished post construct");
	}
	/**
	 * This is necessary to have the Spring Boot app use the Astra secure bundle
	 * to connect to the database
	 */
	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraConfig astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}
}
