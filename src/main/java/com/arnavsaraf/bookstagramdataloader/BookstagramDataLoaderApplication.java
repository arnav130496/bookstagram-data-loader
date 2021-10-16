package com.arnavsaraf.bookstagramdataloader;

import com.arnavsaraf.bookstagramdataloader.author.model.Author;
import com.arnavsaraf.bookstagramdataloader.author.repository.AuthorRepository;
import com.arnavsaraf.bookstagramdataloader.book.model.Book;
import com.arnavsaraf.bookstagramdataloader.book.repository.BookRepository;
import com.arnavsaraf.bookstagramdataloader.config.DataStaxAstraConfig;
import org.json.JSONArray;
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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraConfig.class)
public class BookstagramDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;
	@Autowired
	BookRepository bookRepository;

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
	private void initWorks(){
		System.out.println("Starting data load");
		Path path = Paths.get(worksDumpLocation);
		DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		try(Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject json = new JSONObject(jsonString);
					List<String> coverIds = new ArrayList<>();
					List<String> authorIds = new ArrayList<>();
					List<String> authorNames = new ArrayList<>();
					String description = "";

					JSONArray coversJsonArr = json.optJSONArray("covers");
					if(coversJsonArr!=null) {
						for (int i = 0; i < coversJsonArr.length(); i++) {
							coverIds.add(coversJsonArr.getString(i));
						}
					}

					JSONArray authJsonArr = json.optJSONArray("authors");
					if(authJsonArr!=null) {
						for (int i = 0; i < authJsonArr.length(); i++) {
							String authId = authJsonArr.getJSONObject(i).getJSONObject("author").getString("key")
									.replace("/authors/","");

							authorIds.add(authId);
							authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
									.map(optAuth -> {
										if(optAuth.isPresent()){return optAuth.get().getName();}
										else{return "Unknown Author";}
									}).collect(Collectors.toList());
						}
					}

					JSONObject descObj = json.optJSONObject("description");
					if(descObj!=null){
						description = descObj.optString("value");
					}
					Book book = Book.builder()
									.id(json.optString("key").replace("/works/",""))
							        .name(json.optString("title"))
									.description(description)
									.coverIds(coverIds)
									.authorIds(authorIds)
									.authorNames(authorNames)
									.publishedDate(LocalDate.parse(json.getJSONObject("created").optString("value"),dateTimeFormatter))
									.build();

					System.out.println("Saving data in cassandra for book "  + book.getName());
					bookRepository.save(book);
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@PostConstruct
	public void start(){
//		initAuthors();
		initWorks();
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
